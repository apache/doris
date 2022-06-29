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
#include "exec/hdfs_file_reader.h"

#include <sys/stat.h>
#include <unistd.h>

#include "common/logging.h"
#include "service/backend_options.h"

namespace doris {

HdfsFileReader::HdfsFileReader(const THdfsParams& hdfs_params, const std::string& path,
                               int64_t start_offset)
        : _hdfs_params(hdfs_params),
          _path(path),
          _current_offset(start_offset),
          _file_size(-1),
          _hdfs_fs(nullptr),
          _hdfs_file(nullptr),
          _builder(createHDFSBuilder(_hdfs_params)) {
    _namenode = _hdfs_params.fs_name;
}

HdfsFileReader::~HdfsFileReader() {
    close();
}

Status HdfsFileReader::connect() {
    if (_builder.is_need_kinit()) {
        RETURN_IF_ERROR(_builder.run_kinit());
    }
    _hdfs_fs = hdfsBuilderConnect(_builder.get());
    if (_hdfs_fs == nullptr) {
        std::stringstream ss;
        ss << "connect failed. " << _namenode;
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

Status HdfsFileReader::open() {
    if (!closed()) {
        close();
    }
    RETURN_IF_ERROR(connect());
    _hdfs_file = hdfsOpenFile(_hdfs_fs, _path.c_str(), O_RDONLY, 0, 0, 0);
    if (_hdfs_file == nullptr) {
        std::stringstream ss;
        ss << "open file failed. "
           << "(BE: " << BackendOptions::get_localhost() << ")"
           << " namenode:" << _namenode << ", path:" << _path << ", err: " << hdfsGetLastError();
        return Status::InternalError(ss.str());
    }
    VLOG_NOTICE << "open file, namenode:" << _namenode << ", path:" << _path;
    return seek(_current_offset);
}

void HdfsFileReader::close() {
    if (!closed()) {
        if (_hdfs_file != nullptr && _hdfs_fs != nullptr) {
            VLOG_NOTICE << "close hdfs file: " << _namenode << _path;
            //If the hdfs file was valid, the memory associated with it will
            // be freed at the end of this call, even if there was an I/O error
            hdfsCloseFile(_hdfs_fs, _hdfs_file);
        }
        if (_hdfs_fs != nullptr) {
            // Even if there is an error, the resources associated with the hdfsFS will be freed.
            hdfsDisconnect(_hdfs_fs);
        }
    }
    _hdfs_file = nullptr;
    _hdfs_fs = nullptr;
}

bool HdfsFileReader::closed() {
    return _hdfs_file == nullptr || _hdfs_fs == nullptr;
}

// Read all bytes
Status HdfsFileReader::read_one_message(std::unique_ptr<uint8_t[]>* buf, int64_t* length) {
    int64_t file_size = size() - _current_offset;
    if (file_size <= 0) {
        buf->reset();
        *length = 0;
        return Status::OK();
    }
    bool eof;
    buf->reset(new uint8_t[file_size]);
    read(buf->get(), file_size, length, &eof);
    return Status::OK();
}

Status HdfsFileReader::read(uint8_t* buf, int64_t buf_len, int64_t* bytes_read, bool* eof) {
    readat(_current_offset, buf_len, bytes_read, buf);
    if (*bytes_read == 0) {
        *eof = true;
    } else {
        *eof = false;
    }
    return Status::OK();
}

Status HdfsFileReader::readat(int64_t position, int64_t nbytes, int64_t* bytes_read, void* out) {
    if (position != _current_offset) {
        int ret = hdfsSeek(_hdfs_fs, _hdfs_file, position);
        if (ret != 0) { // check fseek return value
            std::stringstream ss;
            ss << "hdfsSeek failed. "
               << "(BE: " << BackendOptions::get_localhost() << ")" << _namenode << _path
               << ", err: " << hdfsGetLastError();
            ;
            return Status::InternalError(ss.str());
        }
    }

    *bytes_read = hdfsRead(_hdfs_fs, _hdfs_file, out, nbytes);
    if (*bytes_read < 0) {
        std::stringstream ss;
        ss << "Read hdfs file failed. "
           << "(BE: " << BackendOptions::get_localhost() << ")" << _namenode << _path
           << ", err: " << hdfsGetLastError();
        ;
        return Status::InternalError(ss.str());
    }
    _current_offset += *bytes_read; // save offset with file
    return Status::OK();
}

int64_t HdfsFileReader::size() {
    if (_file_size == -1) {
        bool need_init_fs = false;
        if (_hdfs_fs == nullptr) {
            need_init_fs = true;
            if (!connect().ok()) {
                return -1;
            }
        }
        hdfsFileInfo* file_info = hdfsGetPathInfo(_hdfs_fs, _path.c_str());
        if (file_info == nullptr) {
            LOG(WARNING) << "get path info failed: " << _namenode << _path
                         << ", err: " << hdfsGetLastError();
            ;
            close();
            return -1;
        }
        _file_size = file_info->mSize;
        hdfsFreeFileInfo(file_info, 1);
        if (need_init_fs) {
            close();
        }
    }
    return _file_size;
}

Status HdfsFileReader::seek(int64_t position) {
    int res = hdfsSeek(_hdfs_fs, _hdfs_file, position);
    if (res != 0) {
        std::stringstream ss;
        ss << "Seek to offset failed. "
           << "(BE: " << BackendOptions::get_localhost() << ")"
           << " offset=" << position << ", err: " << hdfsGetLastError();
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

Status HdfsFileReader::tell(int64_t* position) {
    *position = _current_offset;
    return Status::OK();
}

} // namespace doris
