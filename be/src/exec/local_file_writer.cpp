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

#include "exec/local_file_writer.h"

#include "util/error_util.h"

namespace doris {

LocalFileWriter::LocalFileWriter(const std::string& path, int64_t start_offset)
        : _path(path), _start_offset(start_offset), _fp(nullptr) {}

LocalFileWriter::~LocalFileWriter() {
    close();
}

Status LocalFileWriter::open() {
    _fp = fopen(_path.c_str(), "w+");
    if (_fp == nullptr) {
        std::stringstream ss;
        ss << "Open file failed. path=" << _path << ", errno= " << errno
           << ", description=" << get_str_err_msg();
        return Status::InternalError(ss.str());
    }

    if (_start_offset != 0) {
        int success = fseek(_fp, _start_offset, SEEK_SET);
        if (success != 0) {
            std::stringstream ss;
            ss << "Seek to start_offset failed. offset=" << _start_offset << ", errno= " << errno
               << ", description=" << get_str_err_msg();
            return Status::InternalError(ss.str());
        }
    }

    return Status::OK();
}

Status LocalFileWriter::write(const uint8_t* buf, size_t buf_len, size_t* written_len) {
    size_t bytes_written = fwrite(buf, 1, buf_len, _fp);
    if (bytes_written < buf_len) {
        std::stringstream error_msg;
        error_msg << "fail to write to file. "
                  << " len=" << buf_len << ", path=" << _path << ", failed with errno=" << errno
                  << ", description=" << get_str_err_msg();
        return Status::InternalError(error_msg.str());
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

} // end namespace doris
