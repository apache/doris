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

#include <bthread/bthread.h>
// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <fmt/format.h>
#include <glog/logging.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <string>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "io/fs/err_utils.h"
#include "util/async_io.h"
#include "util/doris_metrics.h"

namespace doris {
namespace io {
struct IOContext;

LocalFileReader::LocalFileReader(Path path, size_t file_size, int fd,
                                 std::shared_ptr<LocalFileSystem> fs)
        : _fd(fd), _path(std::move(path)), _file_size(file_size), _fs(std::move(fs)) {
    DorisMetrics::instance()->local_file_open_reading->increment(1);
    DorisMetrics::instance()->local_file_reader_total->increment(1);
}

LocalFileReader::~LocalFileReader() {
    WARN_IF_ERROR(close(), fmt::format("Failed to close file {}", _path.native()));
}

Status LocalFileReader::close() {
    bool expected = false;
    if (_closed.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        DorisMetrics::instance()->local_file_open_reading->increment(-1);
        int res = -1;
#if !defined(USE_BTHREAD_SCANNER)
        DCHECK(bthread_self() == 0);
        res = ::close(_fd);
#else
        if (bthread_self() == 0) {
            res = ::close(_fd);
        } else {
            auto task = [&] { res = ::close(_fd); };
            AsyncIO::run_task(task, io::FileSystemType::LOCAL);
        }
#endif
        if (-1 == res) {
            std::string err = errno_to_str();
            LOG(WARNING) << fmt::format("failed to close {}: {}", _path.native(), err);
            return Status::IOError("failed to close {}: {}", _path.native(), err);
        }
        _fd = -1;
    }
    return Status::OK();
}

Status LocalFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                     const IOContext* /*io_ctx*/) {
    DCHECK(!closed());
    if (offset > _file_size) {
        return Status::IOError("offset exceeds file size(offset: {}, file size: {}, path: {})",
                               offset, _file_size, _path.native());
    }
    size_t bytes_req = result.size;
    char* to = result.data;
    bytes_req = std::min(bytes_req, _file_size - offset);
    *bytes_read = 0;

    while (bytes_req != 0) {
        auto res = ::pread(_fd, to, bytes_req, offset);
        if (UNLIKELY(-1 == res && errno != EINTR)) {
            return Status::IOError("cannot read from {}: {}", _path.native(), std::strerror(errno));
        }
        if (UNLIKELY(res == 0)) {
            return Status::IOError("cannot read from {}: unexpected EOF", _path.native());
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
