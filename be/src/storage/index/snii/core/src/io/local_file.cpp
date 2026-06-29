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

#include "snii/io/local_file.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

namespace snii::io {
using doris::Status; // RETURN_IF_ERROR expands to bare Status
namespace {

std::string errno_msg(const char* what) {
    return std::string(what) + ": " + std::strerror(errno);
}

} // namespace

LocalFileReader::~LocalFileReader() {
    if (fd_ >= 0) ::close(fd_);
}

doris::Status LocalFileReader::open(const std::string& path) {
    fd_ = ::open(path.c_str(), O_RDONLY);
    if (fd_ < 0) return doris::Status::Error<doris::ErrorCode::IO_ERROR, false>(errno_msg("open"));
    struct stat st;
    if (::fstat(fd_, &st) != 0) return doris::Status::Error<doris::ErrorCode::IO_ERROR, false>(errno_msg("fstat"));
    size_ = static_cast<uint64_t>(st.st_size);
    return doris::Status::OK();
}

doris::Status LocalFileReader::read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) {
    if (fd_ < 0) return doris::Status::Error<doris::ErrorCode::IO_ERROR, false>("read_at on unopened file");
    // Non-wrapping bounds check (offset+len could overflow uint64 on a corrupt arg).
    if (offset > size_ || len > size_ - offset) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("read_at past end of file");
    }
    out->resize(len);
    size_t done = 0;
    while (done < len) {
        ssize_t n = ::pread(fd_, out->data() + done, len - done, static_cast<off_t>(offset + done));
        if (n < 0) {
            if (errno == EINTR) continue;
            return doris::Status::Error<doris::ErrorCode::IO_ERROR, false>(errno_msg("pread"));
        }
        if (n == 0) return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("pread returned 0 before len");
        done += static_cast<size_t>(n);
    }
    return doris::Status::OK();
}

LocalFileWriter::~LocalFileWriter() {
    if (fd_ >= 0) ::close(fd_); // best-effort: dtor cannot surface a flush error
}

doris::Status LocalFileWriter::open(const std::string& path) {
    fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd_ < 0) return doris::Status::Error<doris::ErrorCode::IO_ERROR, false>(errno_msg("open"));
    buf_.reserve(kBufCapacity);
    return doris::Status::OK();
}

doris::Status LocalFileWriter::write_all(const uint8_t* data, size_t len) {
    size_t done = 0;
    while (done < len) {
        ssize_t n = ::write(fd_, data + done, len - done);
        if (n < 0) {
            if (errno == EINTR) continue;
            return doris::Status::Error<doris::ErrorCode::IO_ERROR, false>(errno_msg("write"));
        }
        done += static_cast<size_t>(n);
    }
    return doris::Status::OK();
}

doris::Status LocalFileWriter::flush_buffer() {
    if (buf_.empty()) return doris::Status::OK();
    RETURN_IF_ERROR(write_all(buf_.data(), buf_.size()));
    buf_.clear();
    return doris::Status::OK();
}

doris::Status LocalFileWriter::append(Slice data) {
    if (fd_ < 0) return doris::Status::Error<doris::ErrorCode::IO_ERROR, false>("append on unopened file");
    const size_t len = data.size();
    if (len == 0) return doris::Status::OK();
    // Spans larger than the buffer go straight to the fd (after flushing pending
    // bytes) to avoid a pointless copy and an oversized buffer.
    if (len >= kBufCapacity) {
        RETURN_IF_ERROR(flush_buffer());
        RETURN_IF_ERROR(write_all(data.data(), len));
        bytes_written_ += len;
        return doris::Status::OK();
    }
    if (buf_.size() + len > kBufCapacity) RETURN_IF_ERROR(flush_buffer());
    buf_.insert(buf_.end(), data.data(), data.data() + len);
    bytes_written_ += len;
    return doris::Status::OK();
}

doris::Status LocalFileWriter::finalize() {
    if (fd_ < 0) return doris::Status::Error<doris::ErrorCode::IO_ERROR, false>("finalize on unopened file");
    RETURN_IF_ERROR(flush_buffer());
    if (::fsync(fd_) != 0) return doris::Status::Error<doris::ErrorCode::IO_ERROR, false>(errno_msg("fsync"));
    if (::close(fd_) != 0) {
        fd_ = -1;
        return doris::Status::Error<doris::ErrorCode::IO_ERROR, false>(errno_msg("close"));
    }
    fd_ = -1;
    return doris::Status::OK();
}

} // namespace snii::io
