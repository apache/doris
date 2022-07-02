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

#include "io/local_file_writer.h"

#include "service/backend_options.h"
#include "util/error_util.h"
#include "util/file_utils.h"

namespace doris {

LocalFileWriter::LocalFileWriter(const std::string& path, int64_t start_offset)
        : _path(path), _start_offset(start_offset), _fp(nullptr) {}

LocalFileWriter::~LocalFileWriter() {
    close();
}

Status LocalFileWriter::open() {
    RETURN_IF_ERROR(_check_file_path(_path));

    _fp = fopen(_path.c_str(), "w+");
    if (_fp == nullptr) {
        return Status::InternalError("Open file failed. path={}, errno= {}, description={}", _path,
                                     errno, get_str_err_msg());
    }

    if (_start_offset != 0) {
        int success = fseek(_fp, _start_offset, SEEK_SET);
        if (success != 0) {
            return Status::InternalError(
                    "Seek to start_offset failed. offset={}, errno= {}, description={}",
                    _start_offset, errno, get_str_err_msg());
        }
    }

    return Status::OK();
}

Status LocalFileWriter::write(const uint8_t* buf, size_t buf_len, size_t* written_len) {
    size_t bytes_written = fwrite(buf, 1, buf_len, _fp);
    if (bytes_written < buf_len) {
        return Status::InternalError(
                "fail to write to file. len={}, path={}, failed with errno={}, description={}",
                buf_len, _path, errno, get_str_err_msg());
    }

    *written_len = bytes_written;
    return Status::OK();
}

Status LocalFileWriter::close() {
    if (_fp != nullptr) {
        fclose(_fp);
        _fp = nullptr;
    }
    return Status::OK();
}

Status LocalFileWriter::_check_file_path(const std::string& file_path) {
    // For local file writer, the file_path is a local dir.
    // Here we do a simple security verification by checking whether the file exists.
    // Because the file path is currently arbitrarily specified by the user,
    // Doris is not responsible for ensuring the correctness of the path.
    // This is just to prevent overwriting the existing file.
    if (FileUtils::check_exist(file_path)) {
        return Status::InternalError("File already exists: {}. Host: {}", file_path,
                                     BackendOptions::get_localhost());
    }

    return Status::OK();
}

} // end namespace doris
