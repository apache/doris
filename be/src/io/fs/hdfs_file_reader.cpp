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

#include "io/fs/hdfs_file_reader.h"

#include "io/fs/hdfs_file_system.h"
#include "service/backend_options.h"
#include "util/doris_metrics.h"
namespace doris {
namespace io {
HdfsFileReader::HdfsFileReader(Path path, size_t file_size, const std::string& name_node,
                               hdfsFile hdfs_file, HdfsFileSystem* fs)
        : _path(std::move(path)),
          _file_size(file_size),
          _name_node(name_node),
          _hdfs_file(hdfs_file),
          _fs(fs) {
    DorisMetrics::instance()->hdfs_file_open_reading->increment(1);
    DorisMetrics::instance()->hdfs_file_reader_total->increment(1);
}

HdfsFileReader::~HdfsFileReader() {
    close();
}

Status HdfsFileReader::close() {
    bool expected = false;
    if (_closed.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        auto handle = _fs->get_handle();
        auto hdfs_fs = handle->hdfs_fs;
        if (_hdfs_file != nullptr && hdfs_fs != nullptr) {
            VLOG_NOTICE << "close hdfs file: " << _name_node << _path;
            // If the hdfs file was valid, the memory associated with it will
            // be freed at the end of this call, even if there was an I/O error
            hdfsCloseFile(hdfs_fs, _hdfs_file);
        }

        DorisMetrics::instance()->hdfs_file_open_reading->increment(-1);
    }
    return Status::OK();
}

Status HdfsFileReader::read_at(size_t offset, Slice result, const IOContext& /*io_ctx*/,
                               size_t* bytes_read) {
    DCHECK(!closed());
    if (offset > _file_size) {
        return Status::IOError("offset exceeds file size(offset: {}, file size: {}, path: {})",
                               offset, _file_size, _path.native());
    }

    auto handle = _fs->get_handle();
    int res = hdfsSeek(handle->hdfs_fs, _hdfs_file, offset);
    if (res != 0) {
        return Status::InternalError("Seek to offset failed. (BE: {}) offset={}, err: {}",
                                     BackendOptions::get_localhost(), offset, hdfsGetLastError());
    }

    size_t bytes_req = result.size;
    char* to = result.data;
    bytes_req = std::min(bytes_req, _file_size - offset);
    *bytes_read = 0;
    if (UNLIKELY(bytes_req == 0)) {
        return Status::OK();
    }

    size_t has_read = 0;
    while (has_read < bytes_req) {
        int64_t loop_read =
                hdfsRead(handle->hdfs_fs, _hdfs_file, to + has_read, bytes_req - has_read);
        if (loop_read < 0) {
            return Status::InternalError(
                    "Read hdfs file failed. (BE: {}) namenode:{}, path:{}, err: {}",
                    BackendOptions::get_localhost(), _name_node, _path.string(),
                    hdfsGetLastError());
        }
        if (loop_read == 0) {
            break;
        }
        has_read += loop_read;
    }
    *bytes_read = has_read;
    return Status::OK();
}
} // namespace io
} // namespace doris
