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

#pragma once

#include <string>
#include <system_error>

#include "common/status.h"

namespace doris {
namespace io {

std::string errno_to_str();
std::string errcode_to_str(const std::error_code& ec);
std::string hdfs_error();

template <typename... Args>
Status error_from_errno_impl(int _errno, std::string_view msg, Args&&... args) {
    int error_code = ErrorCode::IO_ERROR;
    switch (_errno) {
    case ENOENT:
        // No such file or directory.
        error_code = ErrorCode::FILE_OR_DIR_NOT_EXIST;
        break;
    case EIO:
        // I/O error.
        error_code = ErrorCode::EIO_ERROR;
        break;
    case EEXIST:
        // File exists.
        error_code = ErrorCode::FILE_ALREADY_EXIST;
        break;
    case EACCES:
        // Permission denied.
        error_code = ErrorCode::PERMISSION_DENIED;
        break;
    }
    return Status::Error(error_code, msg, args...);
}

template <typename... Args>
Status error_from_errno(std::string_view msg, Args&&... args) {
    return error_from_errno_impl(errno, msg, args...);
}

template <typename... Args>
Status error_from_ec(const std::error_code& ec, std::string_view msg, Args&&... args) {
    return error_from_errno_impl(ec.value(), msg, args...);
}

} // namespace io
} // namespace doris
