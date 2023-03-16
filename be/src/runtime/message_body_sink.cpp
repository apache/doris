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

#include "runtime/message_body_sink.h"

#include <fcntl.h>

#include <algorithm>

#include "util/runtime_profile.h"

namespace doris {

MessageBodyFileSink::~MessageBodyFileSink() {
    if (_fd >= 0) {
        close(_fd);
    }
}

Status MessageBodyFileSink::open() {
    _fd = ::open(_path.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (_fd < 0) {
        char errmsg[64];
        LOG(WARNING) << "fail to open file, file=" << _path
                     << ", errmsg=" << strerror_r(errno, errmsg, 64);
        return Status::InternalError("fail to open file");
    }
    return Status::OK();
}

Status MessageBodyFileSink::append(const char* data, size_t size) {
    auto written = ::write(_fd, data, size);
    if (written == size) {
        return Status::OK();
    }
    char errmsg[64];
    LOG(WARNING) << "fail to write, file=" << _path << ", error=" << strerror_r(errno, errmsg, 64);
    return Status::InternalError("fail to write file");
}

Status MessageBodyFileSink::finish() {
    if (::close(_fd) < 0) {
        std::stringstream ss;
        char errmsg[64];
        LOG(WARNING) << "fail to write, file=" << _path
                     << ", error=" << strerror_r(errno, errmsg, 64);
        _fd = -1;
        return Status::InternalError("fail to close file");
    }
    _fd = -1;
    return Status::OK();
}

void MessageBodyFileSink::cancel(const std::string& reason) {
    unlink(_path.data());
}

} // namespace doris
