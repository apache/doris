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

#include "io/fs/err_utils.h"

// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <fmt/format.h>
#include <string.h>

#include <sstream>

#include "io/fs/hdfs.h"

namespace doris {
namespace io {

std::string errno_to_str() {
    char buf[1024];
    return fmt::format("({}), {}", errno, strerror_r(errno, buf, 1024));
}

std::string errcode_to_str(const std::error_code& ec) {
    return fmt::format("({}), {}", ec.value(), ec.message());
}

std::string hdfs_error() {
    std::stringstream ss;
    char buf[1024];
    ss << "(" << errno << "), " << strerror_r(errno, buf, 1024) << ")";
#ifdef USE_HADOOP_HDFS
    char* root_cause = hdfsGetLastExceptionRootCause();
    if (root_cause != nullptr) {
        ss << ", reason: " << root_cause;
    }
#else
    ss << ", reason: " << hdfsGetLastError();
#endif
    return ss.str();
}

std::string glob_err_to_str(int code) {
    std::string msg;
    // https://sites.uclouvain.be/SystInfo/usr/include/glob.h.html
    switch (code) {
    case 1:
        msg = "Ran out of memory";
        break;
    case 2:
        msg = "read error";
        break;
    case 3:
        msg = "No matches found";
        break;
    default:
        msg = "unknown";
        break;
    }
    return fmt::format("({}), {}", code, msg);
}

} // namespace io
} // namespace doris
