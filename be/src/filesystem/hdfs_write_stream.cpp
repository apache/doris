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

#include "filesystem/hdfs_write_stream.h"

namespace doris {

HdfsWriteStream::HdfsWriteStream(hdfsFS fs, hdfsFile file) : _fs(fs), _file(file) {}

HdfsWriteStream::~HdfsWriteStream() {
    close();
}

Status HdfsWriteStream::write(const char* from, size_t put_n) {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    while (put_n) {
        int res = hdfsWrite(_fs, _file, from, put_n);
        if (-1 == res) {
            return Status::IOError(fmt::format("hdfsWrite failed: {}", std::strerror(errno)));
        }
        put_n -= res;
        from += res;
    }
    return Status::OK();
}

Status HdfsWriteStream::sync() {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    if (-1 == hdfsSync(_fs, _file)) {
        return Status::IOError(fmt::format("hdfsSync failed: {}", std::strerror(errno)));
    }
    return Status::OK();
}

Status HdfsWriteStream::close() {
    if (!closed()) {
        if (-1 == hdfsSync(_fs, _file)) {
            return Status::IOError(fmt::format("hdfsSync failed: {}", std::strerror(errno)));
        }
        hdfsCloseFile(_fs, _file);
        _file = nullptr;
        hdfsDisconnect(_fs);
        _fs = nullptr;
        _closed = true;
    }
    return Status::OK();
}

} // namespace doris
