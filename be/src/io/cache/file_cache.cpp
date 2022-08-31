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

#include "io/cache/file_cache.h"

#include "common/config.h"
#include "io/fs/local_file_system.h"

namespace doris {
namespace io {

Status FileCache::download_cache_to_local(const Path& cache_file, const Path& cache_done_file,
                                          io::FileReaderSPtr remote_file_reader, size_t req_size,
                                          size_t offset) {
    LOG(INFO) << "Download cache file from remote file: " << remote_file_reader->path().native()
              << " -> " << cache_file.native() << ". offset: " << offset
              << ", request size: " << req_size;
    io::FileWriterPtr file_writer;
    RETURN_NOT_OK_STATUS_WITH_WARN(
            io::global_local_filesystem()->create_file(cache_file, &file_writer),
            fmt::format("Create local cache file failed: {}", cache_file.native()));
    auto func = [cache_file, cache_done_file, remote_file_reader, req_size,
                 offset](io::FileWriter* file_writer) {
        char* file_buf = ExecEnv::GetInstance()->get_download_cache_buf(
                ExecEnv::GetInstance()->get_serial_download_cache_thread_token());
        size_t count_bytes_read = 0;
        size_t need_req_size = config::download_cache_buffer_size;
        while (count_bytes_read < req_size) {
            memset(file_buf, 0, need_req_size);
            if (req_size - count_bytes_read < config::download_cache_buffer_size) {
                need_req_size = req_size - count_bytes_read;
            }
            Slice file_slice(file_buf, need_req_size);
            size_t bytes_read = 0;
            RETURN_NOT_OK_STATUS_WITH_WARN(
                    remote_file_reader->read_at(offset + count_bytes_read, file_slice, &bytes_read),
                    fmt::format("read remote file failed. {}. offset: {}, request size: {}",
                                remote_file_reader->path().native(), offset + count_bytes_read,
                                need_req_size));
            if (bytes_read != need_req_size) {
                LOG(ERROR) << "read remote file failed: " << remote_file_reader->path().native()
                           << ", bytes read: " << bytes_read
                           << " vs need read size: " << need_req_size;
                return Status::OLAPInternalError(OLAP_ERR_OS_ERROR);
            }
            count_bytes_read += bytes_read;
            RETURN_NOT_OK_STATUS_WITH_WARN(
                    file_writer->append(file_slice),
                    fmt::format("Write local cache file failed: {}", cache_file.native()));
        }
        return Status::OK();
    };
    auto st = func(file_writer.get());
    if (!st.ok()) {
        WARN_IF_ERROR(file_writer->close(),
                      fmt::format("Close local cache file failed: {}", cache_file.native()));
        return st;
    }
    RETURN_NOT_OK_STATUS_WITH_WARN(
            file_writer->close(),
            fmt::format("Close local cache file failed: {}", cache_file.native()));
    io::FileWriterPtr done_file_writer;
    RETURN_NOT_OK_STATUS_WITH_WARN(
            io::global_local_filesystem()->create_file(cache_done_file, &done_file_writer),
            fmt::format("Create local done file failed: {}", cache_done_file.native()));
    RETURN_NOT_OK_STATUS_WITH_WARN(
            done_file_writer->close(),
            fmt::format("Close local done file failed: {}", cache_done_file.native()));
    return Status::OK();
}

} // namespace io
} // namespace doris
