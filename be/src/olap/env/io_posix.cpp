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

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "env/io_posix.h"

#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "common/logging.h"
#include "gutil/macro.h"
#include "gutil/port.h"
#include "util/slice.h"

namespace doris {

static Status do_readv_at(const std::string& filename, uint64_t offset,
                          const Slice* res, size_t res_cnt) {
    // Convert the results into the iovec vector to request
    // and calculate the total bytes requested
    size_t bytes_req = 0;
    struct iovec iov[res_cnt];
    for (size_t i = 0; i < res_cnt; i++) {
        Slice& result = results[i];
        bytes_req += result.size();
        iov[i] = { result.mutable_data(), result.size() };
    }

    uint64_t cur_offset = offset;
    size_t completed_iov = 0;
    size_t rem = bytes_req;
    while (rem > 0) {
        // Never request more than IOV_MAX in one request
        size_t iov_count = std::min(res_cnt - completed_iov, static_cast<size_t>(IOV_MAX));
        ssize_t r;
        RETRY_ON_EINTR(r, preadv(fd, iov + completed_iov, iov_count, cur_offset));

        if (PREDICT_FALSE(r < 0)) {
            // An error: return a non-ok status.
            // TODO(zc): return IOError(filename, errno);
            return Status::InternalError("IOError");
        }

        if (PREDICT_FALSE(r == 0)) {
            // EOF.
            // return Status::EndOfFile(
                // Substitute("EOF trying to read $0 bytes at offset $1", bytes_req, offset));
            return Status::InternalError("EOF");
        }

        if (PREDICT_TRUE(r == rem)) {
            // All requested bytes were read. This is almost always the case.
            return Status::OK()();
        }
        DCHECK_LE(r, rem);
        // Adjust iovec vector based on bytes read for the next request
        ssize_t bytes_rem = r;
        for (size_t i = completed_iov; i < res_cnt; i++) {
            if (bytes_rem >= iov[i].iov_len) {
                // The full length of this iovec was read
                completed_iov++;
                bytes_rem -= iov[i].iov_len;
            } else {
                // Partially read this result.
                // Adjust the iov_len and iov_base to request only the missing data.
                iov[i].iov_base = static_cast<uint8_t *>(iov[i].iov_base) + bytes_rem;
                iov[i].iov_len -= bytes_rem;
                break; // Don't need to adjust remaining iovec's
            }
        }
        cur_offset += r;
        rem -= r;
    }
    DCHECK_EQ(0, rem);
    return Status::OK();
}

PosixRandomAccessFile::~PosixRandomAccessFile() {
    int res;
    RETRY_ON_EINTR(res, close(_fd));
    if (res != 0) {
        char buf[64];
        LOG(WARNING) << "close file failed, name=" << _filename
            << ", errno=" << errno << ", msg=" << strerror_r(errno, buf, 64);
    }
}

Status PosixRandomAccessFile::size(uint64_t* size) const {
    struct stat st;
    auto res = fstat(_fd, &st);
    if (res != 0) {
        return Status::InternalError("failed to get file stat");
    }
    *size = st.st_size;
    return Status::OK();
}

}
