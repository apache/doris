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

#include "io/fs/local_file_reader.h"

#include <bthread/bthread.h>
// IWYU pragma: no_include <bthread/errno.h>
#include <bvar/bvar.h>
#include <errno.h> // IWYU pragma: keep
#include <fmt/format.h>
#include <glog/logging.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <string>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "cpp/sync_point.h"
#include "io/fs/err_utils.h"
#include "olap/olap_common.h"
#include "olap/options.h"
#include "util/async_io.h"
#include "util/doris_metrics.h"

namespace doris {
namespace io {

std::vector<doris::DataDirInfo> BeConfDataDirReader::be_config_data_dir_list;

void BeConfDataDirReader::get_data_dir_by_file_path(io::Path* file_path,
                                                    std::string* data_dir_arg) {
    for (const auto& data_dir_info : be_config_data_dir_list) {
        if (data_dir_info.path.size() >= file_path->string().size()) {
            continue;
        }
        if (file_path->string().compare(0, data_dir_info.path.size(), data_dir_info.path) == 0) {
            *data_dir_arg = data_dir_info.path;
        }
    }
}

void BeConfDataDirReader::init_be_conf_data_dir(
        const std::vector<doris::StorePath>& store_paths,
        const std::vector<doris::StorePath>& spill_store_paths) {
    for (int i = 0; i < store_paths.size(); i++) {
        DataDirInfo data_dir_info;
        data_dir_info.path = store_paths[i].path;
        data_dir_info.storage_medium = store_paths[i].storage_medium;
        data_dir_info.data_dir_type = DataDirType::OLAP_DATA_DIR;
        be_config_data_dir_list.push_back(data_dir_info);
    }

    for (int i = 0; i < spill_store_paths.size(); i++) {
        doris::DataDirInfo data_dir_info;
        data_dir_info.path = spill_store_paths[i].path;
        data_dir_info.storage_medium = spill_store_paths[i].storage_medium;
        data_dir_info.data_dir_type = doris::DataDirType::SPILL_DISK_DIR;
        be_config_data_dir_list.push_back(data_dir_info);
    }
}

LocalFileReader::LocalFileReader(Path path, size_t file_size, int fd)
        : _fd(fd), _path(std::move(path)), _file_size(file_size) {
    _data_dir_path = "";
    BeConfDataDirReader::get_data_dir_by_file_path(&_path, &_data_dir_path);
    DorisMetrics::instance()->local_file_open_reading->increment(1);
    DorisMetrics::instance()->local_file_reader_total->increment(1);
}

LocalFileReader::~LocalFileReader() {
    WARN_IF_ERROR(close(), fmt::format("Failed to close file {}", _path.native()));
}

Status LocalFileReader::close() {
    bool expected = false;
    if (_closed.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        DorisMetrics::instance()->local_file_open_reading->increment(-1);
        DCHECK(bthread_self() == 0);
        if (-1 == ::close(_fd)) {
            std::string err = errno_to_str();
            return localfs_error(errno, fmt::format("failed to close {}", _path.native()));
        }
        _fd = -1;
    }
    return Status::OK();
}

Status LocalFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                     const IOContext* /*io_ctx*/) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("LocalFileReader::read_at_impl",
                                      Status::IOError("inject io error"));
    if (closed()) [[unlikely]] {
        return Status::InternalError("read closed file: ", _path.native());
    }

    if (offset > _file_size) {
        return Status::InternalError(
                "offset exceeds file size(offset: {}, file size: {}, path: {})", offset, _file_size,
                _path.native());
    }
    size_t bytes_req = result.size;
    char* to = result.data;
    bytes_req = std::min(bytes_req, _file_size - offset);
    *bytes_read = 0;

    while (bytes_req != 0) {
        auto res = SYNC_POINT_HOOK_RETURN_VALUE(::pread(_fd, to, bytes_req, offset),
                                                "LocalFileReader::pread", _fd, to);
        if (UNLIKELY(-1 == res && errno != EINTR)) {
            return localfs_error(errno, fmt::format("failed to read {}", _path.native()));
        }
        if (UNLIKELY(res == 0)) {
            return Status::InternalError("cannot read from {}: unexpected EOF", _path.native());
        }
        if (res > 0) {
            to += res;
            offset += res;
            bytes_req -= res;
            *bytes_read += res;
        }
    }
    DorisMetrics::instance()->local_bytes_read_total->increment(*bytes_read);
    return Status::OK();
}

} // namespace io
} // namespace doris
