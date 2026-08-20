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

#include "storage/index/snii/bkd/staged_blob_file.h"

#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstring>

#include "common/check.h"
#include "storage/index/snii/writer/temp_dir.h"

namespace doris::snii::bkd {

Status StagedBlobFile::create(const std::string& tag, std::unique_ptr<StagedBlobFile>* out) {
    DORIS_CHECK(out != nullptr);
    // pid plus a process-wide counter: two builds in one BE must not collide, and
    // a leftover from a dead process must not be mistaken for ours.
    static std::atomic<uint64_t> sequence {0};
    std::string path = writer::resolve_temp_dir() + "/snii_bkdstage_" + tag + "_" +
                       std::to_string(::getpid()) + "_" + std::to_string(sequence.fetch_add(1)) +
                       ".stage";

    // O_EXCL: the name is supposed to be unique, so an existing file means the
    // assumption is wrong and silently truncating someone else's staging file
    // would be the worst possible response.
    const int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
    if (fd < 0) {
        return Status::IOError("failed to create blob staging file {}: {}", path,
                               std::strerror(errno));
    }
    std::unique_ptr<StagedBlobFile> file(new StagedBlobFile());
    file->fd_ = fd;
    file->path_ = std::move(path);
    *out = std::move(file);
    return Status::OK();
}

StagedBlobFile::~StagedBlobFile() {
    remove();
}

Status StagedBlobFile::append(Slice data) {
    DORIS_CHECK_GE(fd_, 0);
    DORIS_CHECK(!finalized_);
    const uint8_t* cursor = data.data();
    size_t remaining = data.size();
    while (remaining > 0) {
        const ssize_t written = ::write(fd_, cursor, remaining);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            return Status::IOError("failed to write a blob staging file: {}", std::strerror(errno));
        }
        cursor += written;
        remaining -= static_cast<size_t>(written);
    }
    bytes_written_ += data.size();
    return Status::OK();
}

Status StagedBlobFile::finalize() {
    DORIS_CHECK_GE(fd_, 0);
    DORIS_CHECK(!finalized_);
    // The descriptor stays open on purpose: read_at reads through this same one,
    // so the file survives an unlink and cannot be swapped out from under us.
    // A deferred write error would surface at close(), which happens in remove()
    // after the container has already sealed -- so force it out here instead.
    if (::fsync(fd_) != 0) {
        return Status::IOError("failed to flush a blob staging file: {}", std::strerror(errno));
    }
    finalized_ = true;
    return Status::OK();
}

Status StagedBlobFile::read_at(uint64_t offset, size_t len, uint8_t* out) const {
    DORIS_CHECK_GE(fd_, 0);
    DORIS_CHECK(finalized_);
    if (len == 0) {
        return Status::OK();
    }
    DORIS_CHECK(out != nullptr);
    // Reported, not asserted: the extent comes from the blob file table the
    // container is assembling, and a mismatch there must not take the process
    // down. Written as a subtraction so a huge len cannot wrap the sum.
    if (offset > bytes_written_ || len > bytes_written_ - offset) {
        return Status::IOError("blob staging read [{}, +{}) is outside the staged {} bytes", offset,
                               len, bytes_written_);
    }

    size_t filled = 0;
    while (filled < len) {
        const ssize_t bytes =
                ::pread(fd_, out + filled, len - filled, static_cast<off_t>(offset + filled));
        if (bytes < 0) {
            if (errno == EINTR) {
                continue;
            }
            return Status::IOError("failed to read a blob staging file: {}", std::strerror(errno));
        }
        if (bytes == 0) {
            // The bound above already proved these bytes exist, so EOF here means
            // the file was truncated under us. Never a short success: the caller
            // checksums this buffer.
            return Status::IOError("blob staging file is shorter than the {} bytes it staged",
                                   bytes_written_);
        }
        filled += static_cast<size_t>(bytes);
    }
    return Status::OK();
}

void StagedBlobFile::remove() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    if (!path_.empty()) {
        ::unlink(path_.c_str());
        path_.clear();
    }
}

} // namespace doris::snii::bkd
