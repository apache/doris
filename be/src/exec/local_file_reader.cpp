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

#include "exec/local_file_reader.h"

namespace doris {

LocalFileReader::LocalFileReader(const std::string& path, int64_t start_offset) 
        : _path(path), _start_offset(start_offset), _eof(false), _fp(nullptr) {
}

LocalFileReader::~LocalFileReader() {
    close();
}

Status LocalFileReader::open() {
    _fp = fopen(_path.c_str(), "r");
    if (_fp == nullptr) {
        char err_buf[64];
        std::stringstream ss;
        ss << "Open file failed. path=" << _path 
            << ", error=" << strerror_r(errno, err_buf, 64);
        return Status::InternalError(ss.str());
    }

    if (_start_offset != 0) {
        int res = fseek(_fp, _start_offset, SEEK_SET);
        if (res != 0) {
            char err_buf[64];
            std::stringstream ss;
            ss << "Seek to start_offset failed. offset=" << _start_offset
                << ", error=" << strerror_r(errno, err_buf, 64);
            return Status::InternalError(ss.str());
        }
    }

    return Status::OK();
}

Status LocalFileReader::read(uint8_t* buf, size_t* buf_len, bool* eof) {
    if (_eof) {
        *buf_len = 0;
        *eof = true;
        return Status::OK();
    }
    size_t read_len = fread(buf, 1, *buf_len, _fp);
    if (read_len < *buf_len) {
        if (ferror(_fp)) {
            char err_buf[64];
            std::stringstream ss;
            ss << "Read file failed. path=" << _path 
                << ", error=" << strerror_r(errno, err_buf, 64);
            return Status::InternalError(ss.str());
        } else if (feof(_fp)) {
            *buf_len = read_len;
            _eof = true;
            if (*buf_len == 0) {
                *eof = true;
            }
        } else {
            return Status::InternalError("Unknown read failed.");
        }
    }
    return Status::OK();
}

void LocalFileReader::close() {
    if (_fp != nullptr) {
        fclose(_fp);
        _fp = nullptr;
    }
}

}
