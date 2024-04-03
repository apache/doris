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

#include <filesystem>
#include <ostream>
#include <string>
#include <utility>

#include "common/logging.h"
#include "common/status.h"
#include "io/fs/err_utils.h"
#include "io/fs/hdfs_file_system.h"
#include "io/hdfs_util.h"
#include "service/backend_options.h"

namespace doris::io {

HdfsFileWriter::HdfsFileWriter(Path path, HdfsHandler* handler, hdfsFile hdfs_file,
                               std::string fs_name)
        : _path(std::move(path)),
          _hdfs_handler(handler),
          _hdfs_file(hdfs_file),
          _fs_name(std::move(fs_name)) {}

HdfsFileWriter::~HdfsFileWriter() {
    if (_hdfs_handler->from_cache) {
        _hdfs_handler->dec_ref();
    } else {
        delete _hdfs_handler;
    }
}

Status HdfsFileWriter::close() {
    if (_closed) {
        return Status::OK();
    }
    _closed = true;
    int result = hdfsFlush(_hdfs_handler->hdfs_fs, _hdfs_file);
    if (result == -1) {
        std::stringstream ss;
        ss << "failed to flush hdfs file. "
           << "fs_name:" << _fs_name << " path:" << _path << ", err: " << hdfs_error();
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    hdfsCloseFile(_hdfs_handler->hdfs_fs, _hdfs_file);
    _hdfs_file = nullptr;
    return Status::OK();
}

Status HdfsFileWriter::appendv(const Slice* data, size_t data_cnt) {
    if (_closed) [[unlikely]] {
        return Status::InternalError("append to closed file: {}", _path.native());
    }

    for (size_t i = 0; i < data_cnt; i++) {
        const Slice& result = data[i];
        size_t left_bytes = result.size;
        const char* p = result.data;
        while (left_bytes > 0) {
            int64_t written_bytes = hdfsWrite(_hdfs_handler->hdfs_fs, _hdfs_file, p, left_bytes);
            if (written_bytes < 0) {
                return Status::InternalError("write hdfs failed. fs_name: {}, path: {}, error: {}",
                                             _fs_name, _path.native(), hdfs_error());
            }
            left_bytes -= written_bytes;
            p += written_bytes;
            _bytes_appended += written_bytes;
        }
    }
    return Status::OK();
}

// Call this method when there is no more data to write.
Status HdfsFileWriter::finalize() {
    if (_closed) [[unlikely]] {
        return Status::InternalError("finalize closed file: {}", _path.native());
    }
    // FIXME(plat1ko): `finalize` method should not be an operation which can be blocked for a long time
    return close();
}

Result<FileWriterPtr> HdfsFileWriter::create(Path full_path, HdfsHandler* handler,
                                             const std::string& fs_name) {
    auto path = convert_path(full_path, fs_name);
    std::string hdfs_dir = path.parent_path().string();
    int exists = hdfsExists(handler->hdfs_fs, hdfs_dir.c_str());
    if (exists != 0) {
        // FIXME(plat1ko): Directly return error here?
        VLOG_NOTICE << "hdfs dir doesn't exist, create it: " << hdfs_dir;
        int ret = hdfsCreateDirectory(handler->hdfs_fs, hdfs_dir.c_str());
        if (ret != 0) {
            // TODO(plat1ko): Normalized error handling
            std::stringstream ss;
            ss << "create dir failed. "
               << " fs_name: " << fs_name << " path: " << hdfs_dir << ", err: " << hdfs_error();
            LOG(WARNING) << ss.str();
            return ResultError(Status::InternalError(ss.str()));
        }
    }
    // open file
    auto* hdfs_file = hdfsOpenFile(handler->hdfs_fs, path.c_str(), O_WRONLY, 0, 0, 0);
    if (hdfs_file == nullptr) {
        std::stringstream ss;
        ss << "open file failed. "
           << " fs_name:" << fs_name << " path:" << path << ", err: " << hdfs_error();
        LOG(WARNING) << ss.str();
        return ResultError(Status::InternalError(ss.str()));
    }
    VLOG_NOTICE << "open file. fs_name:" << fs_name << ", path:" << path;
    return std::make_unique<HdfsFileWriter>(std::move(path), handler, hdfs_file, fs_name);
}

} // namespace doris::io
