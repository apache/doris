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

#include "io/fs/local_file_writer.h"

// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <fcntl.h>
#include <glog/logging.h>
#include <limits.h>
#include <stdint.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <ostream>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "gutil/macros.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "util/doris_metrics.h"

namespace doris {
namespace detail {

Status sync_dir(const io::Path& dirname) {
    int fd;
    RETRY_ON_EINTR(fd, ::open(dirname.c_str(), O_DIRECTORY | O_RDONLY));
    if (-1 == fd) {
        return Status::IOError("cannot open {}: {}", dirname.native(), std::strerror(errno));
    }
#ifdef __APPLE__
    if (fcntl(fd, F_FULLFSYNC) < 0) {
        return Status::IOError("cannot sync {}: {}", dirname.native(), std::strerror(errno));
    }
#else
    if (0 != ::fdatasync(fd)) {
        return Status::IOError("cannot fdatasync {}: {}", dirname.native(), std::strerror(errno));
    }
#endif
    ::close(fd);
    return Status::OK();
}

} // namespace detail

namespace io {

LocalFileWriter::LocalFileWriter(Path path, int fd, FileSystemSPtr fs, bool sync_data)
        : FileWriter(std::move(path), fs), _fd(fd), _sync_data(sync_data) {
    _opened = true;
    DorisMetrics::instance()->local_file_open_writing->increment(1);
    DorisMetrics::instance()->local_file_writer_total->increment(1);
}

LocalFileWriter::LocalFileWriter(Path path, int fd)
        : LocalFileWriter(path, fd, global_local_filesystem()) {}

LocalFileWriter::~LocalFileWriter() {
    if (_opened) {
        close();
    }
    CHECK(!_opened || _closed) << "open: " << _opened << ", closed: " << _closed;
}

Status LocalFileWriter::close() {
    return _close(_sync_data);
}

Status LocalFileWriter::abort() {
    RETURN_IF_ERROR(_close(false));
    return io::global_local_filesystem()->delete_file(_path);
}

Status LocalFileWriter::appendv(const Slice* data, size_t data_cnt) {
    DCHECK(!_closed);
    _dirty = true;

    // Convert the results into the iovec vector to request
    // and calculate the total bytes requested.
    size_t bytes_req = 0;
    struct iovec iov[data_cnt];
    for (size_t i = 0; i < data_cnt; i++) {
        const Slice& result = data[i];
        bytes_req += result.size;
        iov[i] = {result.data, result.size};
    }

    size_t completed_iov = 0;
    size_t n_left = bytes_req;
    while (n_left > 0) {
        // Never request more than IOV_MAX in one request.
        size_t iov_count = std::min(data_cnt - completed_iov, static_cast<size_t>(IOV_MAX));
        ssize_t res;
        RETRY_ON_EINTR(res, ::writev(_fd, iov + completed_iov, iov_count));
        if (UNLIKELY(res < 0)) {
            return Status::IOError("cannot write to {}: {}", _path.native(), std::strerror(errno));
        }

        if (LIKELY(res == n_left)) {
            // All requested bytes were read. This is almost always the case.
            n_left = 0;
            break;
        }
        // Adjust iovec vector based on bytes read for the next request.
        ssize_t bytes_rem = res;
        for (size_t i = completed_iov; i < data_cnt; i++) {
            if (bytes_rem >= iov[i].iov_len) {
                // The full length of this iovec was written.
                completed_iov++;
                bytes_rem -= iov[i].iov_len;
            } else {
                // Partially wrote this result.
                // Adjust the iov_len and iov_base to write only the missing data.
                iov[i].iov_base = static_cast<uint8_t*>(iov[i].iov_base) + bytes_rem;
                iov[i].iov_len -= bytes_rem;
                break; // Don't need to adjust remaining iovec's.
            }
        }
        n_left -= res;
    }
    DCHECK_EQ(0, n_left);
    _bytes_appended += bytes_req;
    return Status::OK();
}

Status LocalFileWriter::write_at(size_t offset, const Slice& data) {
    DCHECK(!_closed);
    _dirty = true;

    size_t bytes_req = data.size;
    char* from = data.data;

    while (bytes_req != 0) {
        auto res = ::pwrite(_fd, from, bytes_req, offset);
        if (-1 == res && errno != EINTR) {
            return Status::IOError("cannot write to {}: {}", _path.native(), std::strerror(errno));
        }
        if (res > 0) {
            from += res;
            bytes_req -= res;
        }
    }
    return Status::OK();
}

Status LocalFileWriter::finalize() {
    DCHECK(!_closed);
    if (_dirty) {
#if defined(__linux__)
        int flags = SYNC_FILE_RANGE_WRITE;
        if (sync_file_range(_fd, 0, 0, flags) < 0) {
            return Status::IOError("cannot sync {}: {}", _path.native(), std::strerror(errno));
        }
#endif
    }
    return Status::OK();
}

Status LocalFileWriter::_close(bool sync) {
    if (_closed) {
        return Status::OK();
    }
    _closed = true;
    if (sync && _dirty) {
#ifdef __APPLE__
        if (fcntl(_fd, F_FULLFSYNC) < 0) {
            return Status::IOError("cannot sync {}: {}", _path.native(), std::strerror(errno));
        }
#else
        if (0 != ::fdatasync(_fd)) {
            return Status::IOError("cannot fdatasync {}: {}", _path.native(), std::strerror(errno));
        }
#endif
        RETURN_IF_ERROR(detail::sync_dir(_path.parent_path()));
        _dirty = false;
    }

    DorisMetrics::instance()->local_file_open_writing->increment(-1);
    DorisMetrics::instance()->file_created_total->increment(1);
    DorisMetrics::instance()->local_bytes_written_total->increment(_bytes_appended);

    if (0 != ::close(_fd)) {
        return Status::IOError("cannot close {}: {}", _path.native(), std::strerror(errno));
    }
    return Status::OK();
}

} // namespace io
} // namespace doris
