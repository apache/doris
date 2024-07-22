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
#include "cpp/sync_point.h"
#include "gutil/macros.h"
#include "io/fs/err_utils.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "olap/data_dir.h"
#include "util/debug_points.h"
#include "util/doris_metrics.h"

namespace doris::io {
namespace {

Status sync_dir(const io::Path& dirname) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("sync_dir", Status::IOError(""));
    int fd;
    RETRY_ON_EINTR(fd, ::open(dirname.c_str(), O_DIRECTORY | O_RDONLY));
    if (-1 == fd) {
        return localfs_error(errno, fmt::format("failed to open {}", dirname.native()));
    }
    Defer defer {[fd] { ::close(fd); }};
#ifdef __APPLE__
    if (fcntl(fd, F_FULLFSYNC) < 0) {
        return localfs_error(errno, fmt::format("failed to sync {}", dirname.native()));
    }
#else
    if (0 != ::fdatasync(fd)) {
        return localfs_error(errno, fmt::format("failed to sync {}", dirname.native()));
    }
#endif
    return Status::OK();
}

} // namespace

LocalFileWriter::LocalFileWriter(Path path, int fd, bool sync_data)
        : _path(std::move(path)), _fd(fd), _sync_data(sync_data) {
    DorisMetrics::instance()->local_file_open_writing->increment(1);
    DorisMetrics::instance()->local_file_writer_total->increment(1);
}

size_t LocalFileWriter::bytes_appended() const {
    return _bytes_appended;
}

LocalFileWriter::~LocalFileWriter() {
    if (_state == State::OPENED) {
        _abort();
    }
    DorisMetrics::instance()->local_file_open_writing->increment(-1);
    DorisMetrics::instance()->file_created_total->increment(1);
    DorisMetrics::instance()->local_bytes_written_total->increment(_bytes_appended);
}

Status LocalFileWriter::close(bool non_block) {
    if (_state == State::CLOSED) {
        return Status::InternalError("LocalFileWriter already closed, file path {}",
                                     _path.native());
    }
    if (_state == State::ASYNC_CLOSING) {
        if (non_block) {
            return Status::InternalError("Don't submit async close multi times");
        }
        // Actucally the first time call to close(true) would return the value of _finalize, if it returned one
        // error status then the code would never call the second close(true)
        _state = State::CLOSED;
        return Status::OK();
    }
    if (non_block) {
        _state = State::ASYNC_CLOSING;
    } else {
        _state = State::CLOSED;
    }
    return _close(_sync_data);
}

void LocalFileWriter::_abort() {
    auto st = _close(false);
    if (!st.ok()) [[unlikely]] {
        LOG(WARNING) << "close file failed when abort file writer: " << st;
    }
    st = io::global_local_filesystem()->delete_file(_path);
    if (!st.ok()) [[unlikely]] {
        LOG(WARNING) << "delete file failed when abort file writer: " << st;
    }
}

Status LocalFileWriter::appendv(const Slice* data, size_t data_cnt) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("LocalFileWriter::appendv",
                                      Status::IOError("inject io error"));
    if (_state != State::OPENED) [[unlikely]] {
        return Status::InternalError("append to closed file: {}", _path.native());
    }
    _dirty = true;

    // Convert the results into the iovec vector to request
    // and calculate the total bytes requested.
    size_t bytes_req = 0;
    std::vector<iovec> iov(data_cnt);
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
        RETRY_ON_EINTR(res, SYNC_POINT_HOOK_RETURN_VALUE(
                                    ::writev(_fd, iov.data() + completed_iov, iov_count),
                                    "LocalFileWriter::writev", _fd));
        if (UNLIKELY(res < 0)) {
            return localfs_error(errno, fmt::format("failed to write {}", _path.native()));
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

// TODO(ByteYue): Refactor this function as FileWriter::flush()
Status LocalFileWriter::_finalize() {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("LocalFileWriter::finalize",
                                      Status::IOError("inject io error"));
    if (_state == State::OPENED) [[unlikely]] {
        return Status::InternalError("finalize closed file: {}", _path.native());
    }

    if (_dirty) {
#if defined(__linux__)
        int flags = SYNC_FILE_RANGE_WRITE;
        if (sync_file_range(_fd, 0, 0, flags) < 0) {
            return localfs_error(errno, fmt::format("failed to finalize {}", _path.native()));
        }
#endif
    }
    return Status::OK();
}

Status LocalFileWriter::_close(bool sync) {
    auto fd_reclaim_func = [&](Status st) {
        if (_fd > 0 && 0 != ::close(_fd)) {
            return localfs_error(errno, fmt::format("failed to {}, along with failed to close {}",
                                                    st, _path.native()));
        }
        _fd = -1;
        return st;
    };
    if (sync && config::sync_file_on_close) {
        if (_dirty) {
#ifdef __APPLE__
            if (fcntl(_fd, F_FULLFSYNC) < 0) [[unlikely]] {
                return fd_reclaim_func(
                        localfs_error(errno, fmt::format("failed to sync {}", _path.native())));
            }
#else
            if (0 != ::fdatasync(_fd)) [[unlikely]] {
                return fd_reclaim_func(
                        localfs_error(errno, fmt::format("failed to sync {}", _path.native())));
            }
#endif
            _dirty = false;
        }
        RETURN_IF_ERROR(fd_reclaim_func(sync_dir(_path.parent_path())));
    }

    DBUG_EXECUTE_IF("LocalFileWriter.close.failed", {
        // spare '.testfile' to make bad disk checker happy
        if (_path.filename().compare(kTestFilePath)) {
            return fd_reclaim_func(
                    Status::IOError("cannot close {}: {}", _path.native(), std::strerror(errno)));
        }
    });

    TEST_SYNC_POINT_RETURN_WITH_VALUE("LocalFileWriter::close", Status::IOError("inject io error"));
    return fd_reclaim_func(Status::OK());
}

} // namespace doris::io
