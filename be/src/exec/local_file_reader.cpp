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

#include <sys/stat.h>
#include <unistd.h>

#include "common/logging.h"

namespace doris {

LocalFileReader::LocalFileReader(const std::string& path, int64_t start_offset)
        : _path(path), _current_offset(start_offset), _file_size(-1), _fp(nullptr) {}

LocalFileReader::~LocalFileReader() {
    close();
}

Status LocalFileReader::open() {
    _fp = fopen(_path.c_str(), "r");
    if (_fp == nullptr) {
        char err_buf[64];
        std::stringstream ss;
        ss << "Open file failed. path=" << _path << ", error=" << strerror_r(errno, err_buf, 64);
        return Status::InternalError(ss.str());
    }
    return seek(_current_offset);
}

void LocalFileReader::close() {
    if (_fp != nullptr) {
        fclose(_fp);
        _fp = nullptr;
    }
}

bool LocalFileReader::closed() {
    return _fp == nullptr;
}

// Read all bytes
Status LocalFileReader::read_one_message(std::unique_ptr<uint8_t[]>* buf, int64_t* length) {
    bool eof;
    int64_t file_size = size() - _current_offset;
    if (file_size <= 0) {
        buf->reset();
        *length = 0;
        return Status::OK();
    }
    buf->reset(new uint8_t[file_size]);
    read(buf->get(), file_size, length, &eof);
    return Status::OK();
}

Status LocalFileReader::read(uint8_t* buf, int64_t buf_len, int64_t* bytes_read, bool* eof) {
    readat(_current_offset, buf_len, bytes_read, buf);
    if (*bytes_read == 0) {
        *eof = true;
    } else {
        *eof = false;
    }
    return Status::OK();
}

Status LocalFileReader::readat(int64_t position, int64_t nbytes, int64_t* bytes_read, void* out) {
    if (position != _current_offset) {
        int ret = fseek(_fp, position, SEEK_SET);
        if (ret != 0) { // check fseek return value
            return Status::InternalError(strerror(errno));
        }
    }

    *bytes_read = fread(out, 1, nbytes, _fp);
    if (*bytes_read == 0 && ferror(_fp)) {
        char err_buf[64];
        std::stringstream ss;
        ss << "Read file failed. path=" << _path << ", error=" << strerror_r(errno, err_buf, 64);
        return Status::InternalError(ss.str());
    }
    _current_offset = ftell(_fp); // save offset with file
    return Status::OK();
}

int64_t LocalFileReader::size() {
    if (_file_size == -1) {
        int ret;
        struct stat buf;
        ret = fstat(fileno(_fp), &buf);
        if (ret) {
            LOG(WARNING) << "Get file size is error, errno: " << errno << ", msg "
                         << strerror(errno);
            return -1;
        }
        _file_size = buf.st_size;
    }
    return _file_size;
}

Status LocalFileReader::seek(int64_t position) {
    int res = fseek(_fp, position, SEEK_SET);
    if (res != 0) {
        char err_buf[64];
        std::stringstream ss;
        ss << "Seek to start_offset failed. offset=" << position
           << ", error=" << strerror_r(errno, err_buf, 64);
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

Status LocalFileReader::tell(int64_t* position) {
    *position = _current_offset;
    return Status::OK();
}

} // namespace doris
