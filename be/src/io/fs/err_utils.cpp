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
#include <aws/s3/S3Errors.h>
#include <errno.h> // IWYU pragma: keep
#include <fmt/format.h>
#include <string.h>

#include <sstream>

#include "common/status.h"
#include "io/fs/hdfs.h"
#include "io/fs/obj_storage_client.h"

namespace doris {
using namespace ErrorCode;

io::ObjectStorageStatus convert_to_obj_response(Status st) {
    int code = st._code;
    std::string msg = st._err_msg == nullptr ? "" : std::move(st._err_msg->_msg);
    return io::ObjectStorageStatus {.code = code, .msg = std::move(msg)};
}

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

Status localfs_error(const std::error_code& ec, std::string_view msg) {
    if (ec == std::errc::io_error) {
        return Status::Error<IO_ERROR, false>(msg);
    } else if (ec == std::errc::no_such_file_or_directory) {
        return Status::Error<NOT_FOUND, false>(msg);
    } else if (ec == std::errc::file_exists) {
        return Status::Error<ALREADY_EXIST, false>(msg);
    } else if (ec == std::errc::no_space_on_device) {
        return Status::Error<DISK_REACH_CAPACITY_LIMIT, false>(msg);
    } else if (ec == std::errc::permission_denied) {
        return Status::Error<PERMISSION_DENIED, false>(msg);
    } else {
        return Status::Error<ErrorCode::INTERNAL_ERROR, false>("{}: {}", msg, ec.message());
    }
}

Status localfs_error(int posix_errno, std::string_view msg) {
    switch (posix_errno) {
    case EIO:
        return Status::Error<IO_ERROR, false>(msg);
    case ENOENT:
        return Status::Error<NOT_FOUND, false>(msg);
    case EEXIST:
        return Status::Error<ALREADY_EXIST, false>(msg);
    case ENOSPC:
        return Status::Error<DISK_REACH_CAPACITY_LIMIT, false>(msg);
    case EACCES:
        return Status::Error<PERMISSION_DENIED, false>(msg);
    default:
        return Status::Error<ErrorCode::INTERNAL_ERROR, false>("{}: {}", msg,
                                                               std::strerror(posix_errno));
    }
}

Status s3fs_error(const Aws::S3::S3Error& err, std::string_view msg) {
    using namespace Aws::Http;
    switch (err.GetResponseCode()) {
    case HttpResponseCode::NOT_FOUND:
        return Status::Error<NOT_FOUND, false>("{}: {} {} type={}, request_id={}", msg,
                                               err.GetExceptionName(), err.GetMessage(),
                                               err.GetErrorType(), err.GetRequestId());
    case HttpResponseCode::FORBIDDEN:
        return Status::Error<PERMISSION_DENIED, false>("{}: {} {} type={}, request_id={}", msg,
                                                       err.GetExceptionName(), err.GetMessage(),
                                                       err.GetErrorType(), err.GetRequestId());
    default:
        return Status::Error<ErrorCode::INTERNAL_ERROR, false>(
                "{}: {} {} code={} type={}, request_id={}", msg, err.GetExceptionName(),
                err.GetMessage(), err.GetResponseCode(), err.GetErrorType(), err.GetRequestId());
    }
}

} // namespace io
} // namespace doris
