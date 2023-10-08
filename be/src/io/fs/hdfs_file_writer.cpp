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

#include "io/fs/hdfs_file_writer.h"

#include <fcntl.h>
#include <stdint.h>

#include <filesystem>
#include <ostream>
#include <string>
#include <utility>

#include "common/logging.h"
#include "io/fs/err_utils.h"
#include "io/fs/hdfs_file_system.h"
#include "service/backend_options.h"
#include "util/hdfs_util.h"

namespace doris {
namespace io {

HdfsFileWriter::HdfsFileWriter(Path file, FileSystemSPtr fs) : FileWriter(std::move(file), fs) {
    _hdfs_fs = (HdfsFileSystem*)_fs.get();
}

HdfsFileWriter::~HdfsFileWriter() {
    if (_opened) {
        static_cast<void>(close());
    }
    CHECK(!_opened || _closed) << "open: " << _opened << ", closed: " << _closed;
}

Status HdfsFileWriter::close() {
    if (_closed) {
        return Status::OK();
    }
    _closed = true;
    if (_hdfs_file == nullptr) {
        return Status::OK();
    }
    int result = hdfsFlush(_hdfs_fs->_fs_handle->hdfs_fs, _hdfs_file);
    if (result == -1) {
        std::stringstream ss;
        ss << "failed to flush hdfs file. "
           << "(BE: " << BackendOptions::get_localhost() << ")"
           << "namenode:" << _hdfs_fs->_fs_name << " path:" << _path << ", err: " << hdfs_error();
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    hdfsCloseFile(_hdfs_fs->_fs_handle->hdfs_fs, _hdfs_file);
    _hdfs_file = nullptr;
    return Status::OK();
}

Status HdfsFileWriter::abort() {
    // TODO: should delete remote file
    return Status::OK();
}

Status HdfsFileWriter::appendv(const Slice* data, size_t data_cnt) {
    DCHECK(!_closed);
    if (!_opened) {
        RETURN_IF_ERROR(_open());
        _opened = true;
    }

    for (size_t i = 0; i < data_cnt; i++) {
        const Slice& result = data[i];
        size_t left_bytes = result.size;
        const char* p = result.data;
        while (left_bytes > 0) {
            int64_t written_bytes =
                    hdfsWrite(_hdfs_fs->_fs_handle->hdfs_fs, _hdfs_file, p, left_bytes);
            if (written_bytes < 0) {
                return Status::InternalError("write hdfs failed. namenode: {}, path: {}, error: {}",
                                             _hdfs_fs->_fs_name, _path.native(), hdfs_error());
            }
            left_bytes -= written_bytes;
            p += written_bytes;
            _bytes_appended += written_bytes;
        }
    }
    return Status::OK();
}

// Call this method when there is no more data to write.
// FIXME(cyx): Does not seem to be an appropriate interface for file system?
Status HdfsFileWriter::finalize() {
    DCHECK(!_closed);
    if (_opened) {
        RETURN_IF_ERROR(close());
    }
    return Status::OK();
}

Status HdfsFileWriter::_open() {
    _path = convert_path(_path, _hdfs_fs->_fs_name);
    std::string hdfs_dir = _path.parent_path().string();
    int exists = hdfsExists(_hdfs_fs->_fs_handle->hdfs_fs, hdfs_dir.c_str());
    if (exists != 0) {
        VLOG_NOTICE << "hdfs dir doesn't exist, create it: " << hdfs_dir;
        int ret = hdfsCreateDirectory(_hdfs_fs->_fs_handle->hdfs_fs, hdfs_dir.c_str());
        if (ret != 0) {
            std::stringstream ss;
            ss << "create dir failed. "
               << "(BE: " << BackendOptions::get_localhost() << ")"
               << " namenode: " << _hdfs_fs->_fs_name << " path: " << hdfs_dir
               << ", err: " << hdfs_error();
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
    }
    // open file
    _hdfs_file = hdfsOpenFile(_hdfs_fs->_fs_handle->hdfs_fs, _path.c_str(), O_WRONLY, 0, 0, 0);
    if (_hdfs_file == nullptr) {
        std::stringstream ss;
        ss << "open file failed. "
           << "(BE: " << BackendOptions::get_localhost() << ")"
           << " namenode:" << _hdfs_fs->_fs_name << " path:" << _path << ", err: " << hdfs_error();
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    VLOG_NOTICE << "open file. namenode:" << _hdfs_fs->_fs_name << ", path:" << _path;
    return Status::OK();
}

} // end namespace io
} // end namespace doris
