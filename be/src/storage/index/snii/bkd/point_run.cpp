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

#include "storage/index/snii/bkd/point_run.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "common/check.h"

namespace doris::snii::bkd {

PointRunWriter::~PointRunWriter() {
    if (fd_ >= 0) {
        // Nothing to report to: a run whose close failed is a run the merge will
        // fail to read, and that failure is the one worth surfacing.
        ::close(fd_);
        fd_ = -1;
    }
}

Status PointRunWriter::open(const std::string& path) {
    DORIS_CHECK_LT(fd_, 0); // one run per writer
    const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0) {
        return Status::IOError("failed to create spilled point run {}: {}", path,
                               std::strerror(errno));
    }
    fd_ = fd;
    return Status::OK();
}

Status PointRunWriter::append(Slice records) {
    DORIS_CHECK_GE(fd_, 0);
    const uint8_t* cursor = records.data();
    size_t remaining = records.size();
    while (remaining > 0) {
        const ssize_t written = ::write(fd_, cursor, remaining);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            return Status::IOError("failed to write a spilled point run: {}", std::strerror(errno));
        }
        // A short write is normal for a large buffer; only a zero-length one on a
        // non-empty request would be a stall, and write(2) does not do that.
        cursor += written;
        remaining -= static_cast<size_t>(written);
    }
    return Status::OK();
}

Status PointRunWriter::close() {
    DORIS_CHECK_GE(fd_, 0);
    const int fd = fd_;
    fd_ = -1;
    if (::close(fd) != 0) {
        // Deferred write errors surface here, so this cannot be ignored: the run
        // would read back short and the merge would silently drop points.
        return Status::IOError("failed to close a spilled point run: {}", std::strerror(errno));
    }
    return Status::OK();
}

PointRunReader::~PointRunReader() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

Status PointRunReader::open(const std::string& path, uint32_t record_size,
                            uint32_t buffer_records) {
    DORIS_CHECK_LT(fd_, 0);
    DORIS_CHECK_GT(record_size, 0U);
    DORIS_CHECK_GT(buffer_records, 0U);

    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        // Not a silently empty stream: swallowing this would drop a whole run
        // from the merge and yield an index that is short with no error anywhere.
        return Status::IOError("failed to open spilled point run {}: {}", path,
                               std::strerror(errno));
    }
    fd_ = fd;
    record_size_ = record_size;
    buffer_.resize(static_cast<size_t>(record_size) * buffer_records);
    return fill();
}

Slice PointRunReader::current() const {
    // Reading past the end is a caller bug -- exhausted() is right there -- so it
    // crashes rather than returning something a merge would happily compare.
    DORIS_CHECK_LE(cursor_ + record_size_, valid_bytes_);
    return {buffer_.data() + cursor_, record_size_};
}

Status PointRunReader::advance() {
    DORIS_CHECK_LE(cursor_ + record_size_, valid_bytes_);
    cursor_ += record_size_;
    if (cursor_ >= valid_bytes_ && !eof_) {
        return fill();
    }
    return Status::OK();
}

Status PointRunReader::fill() {
    valid_bytes_ = 0;
    cursor_ = 0;
    while (valid_bytes_ < buffer_.size()) {
        const ssize_t bytes =
                ::read(fd_, buffer_.data() + valid_bytes_, buffer_.size() - valid_bytes_);
        if (bytes < 0) {
            if (errno == EINTR) {
                continue;
            }
            return Status::IOError("failed to read a spilled point run: {}", std::strerror(errno));
        }
        if (bytes == 0) {
            eof_ = true;
            break;
        }
        valid_bytes_ += static_cast<size_t>(bytes);
    }
    // Records are fixed width and the writer only ever appends whole ones, so a
    // partial tail means the file was truncated after it was written -- a temp
    // dir swept from under us, or a full disk that surfaced late. It is reported
    // rather than asserted because it is reachable without any bug of ours.
    if (valid_bytes_ % record_size_ != 0) {
        return Status::IOError("spilled point run ends mid-record ({} bytes, record size {})",
                               valid_bytes_, record_size_);
    }
    return Status::OK();
}

} // namespace doris::snii::bkd
