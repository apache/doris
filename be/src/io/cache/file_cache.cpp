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
#include "common/status.h"
#include "gutil/strings/util.h"
#include "io/fs/local_file_system.h"
#include "olap/iterators.h"

namespace doris {
using namespace ErrorCode;
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
            IOContext io_ctx;
            RETURN_NOT_OK_STATUS_WITH_WARN(
                    remote_file_reader->read_at(offset + count_bytes_read, file_slice, io_ctx,
                                                &bytes_read),
                    fmt::format("read remote file failed. {}. offset: {}, request size: {}",
                                remote_file_reader->path().native(), offset + count_bytes_read,
                                need_req_size));
            if (bytes_read != need_req_size) {
                LOG(ERROR) << "read remote file failed: " << remote_file_reader->path().native()
                           << ", bytes read: " << bytes_read
                           << " vs need read size: " << need_req_size;
                return Status::Error<OS_ERROR>();
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

Status FileCache::_remove_file(const Path& file, size_t* cleaned_size) {
    bool cache_file_exist = false;
    RETURN_NOT_OK_STATUS_WITH_WARN(io::global_local_filesystem()->exists(file, &cache_file_exist),
                                   "Check local cache file exist failed.");
    size_t file_size = 0;
    if (cache_file_exist) {
        RETURN_NOT_OK_STATUS_WITH_WARN(
                io::global_local_filesystem()->file_size(file, &file_size),
                fmt::format("get local cache file size failed: {}", file.native()));

        RETURN_NOT_OK_STATUS_WITH_WARN(
                io::global_local_filesystem()->delete_file(file),
                fmt::format("Delete local cache file failed: {}", file.native()));
        LOG(INFO) << "Delete local cache file successfully: " << file.native()
                  << ", file size: " << file_size;
    }
    if (cleaned_size) {
        *cleaned_size = file_size;
    }
    return Status::OK();
}

Status FileCache::_remove_cache_and_done(const Path& cache_file, const Path& cache_done_file,
                                         size_t* cleaned_size) {
    RETURN_IF_ERROR(_remove_file(cache_done_file, nullptr));
    RETURN_IF_ERROR(_remove_file(cache_file, cleaned_size));
    return Status::OK();
}

Status FileCache::_get_dir_files_and_remove_unfinished(const Path& cache_dir,
                                                       std::vector<Path>& cache_names) {
    bool cache_dir_exist = true;
    RETURN_NOT_OK_STATUS_WITH_WARN(
            io::global_local_filesystem()->exists(cache_dir, &cache_dir_exist),
            fmt::format("Check local cache dir exist failed. {}", cache_dir.native()));
    if (!cache_dir_exist) {
        return Status::OK();
    }

    // list all files
    std::vector<Path> cache_file_names;
    RETURN_NOT_OK_STATUS_WITH_WARN(
            io::global_local_filesystem()->list(cache_dir, &cache_file_names),
            fmt::format("List dir failed: {}", cache_dir.native()))

    // separate DATA file and DONE file
    std::set<Path> cache_names_temp;
    std::list<Path> done_names_temp;
    for (auto& cache_file_name : cache_file_names) {
        if (ends_with(cache_file_name.native(), CACHE_DONE_FILE_SUFFIX)) {
            done_names_temp.push_back(std::move(cache_file_name));
        } else {
            cache_names_temp.insert(std::move(cache_file_name));
        }
    }

    // match DONE file with DATA file
    for (auto done_file : done_names_temp) {
        Path cache_filename = StringReplace(done_file.native(), CACHE_DONE_FILE_SUFFIX, "", true);
        if (auto cache_iter = cache_names_temp.find(cache_filename);
            cache_iter != cache_names_temp.end()) {
            cache_names_temp.erase(cache_iter);
            cache_names.push_back(std::move(cache_filename));
        } else {
            // not data file, but with DONE file
            RETURN_IF_ERROR(_remove_file(done_file, nullptr));
        }
    }
    // data file without DONE file
    for (auto& file : cache_names_temp) {
        RETURN_IF_ERROR(_remove_file(file, nullptr));
    }
    return Status::OK();
}

Status FileCache::_clean_unfinished_files(const std::vector<Path>& unfinished_files) {
    // remove cache file without done file
    for (auto file : unfinished_files) {
        RETURN_IF_ERROR(_remove_file(file, nullptr));
    }
    return Status::OK();
}

Status FileCache::_check_and_delete_empty_dir(const Path& cache_dir) {
    bool cache_dir_exist = true;
    RETURN_NOT_OK_STATUS_WITH_WARN(
            io::global_local_filesystem()->exists(cache_dir, &cache_dir_exist),
            fmt::format("Check local cache dir exist failed. {}", cache_dir.native()));
    if (!cache_dir_exist) {
        return Status::OK();
    }

    std::vector<Path> cache_file_names;
    RETURN_NOT_OK_STATUS_WITH_WARN(
            io::global_local_filesystem()->list(cache_dir, &cache_file_names),
            fmt::format("List dir failed: {}", cache_dir.native()));
    if (cache_file_names.empty()) {
        RETURN_NOT_OK_STATUS_WITH_WARN(io::global_local_filesystem()->delete_directory(cache_dir),
                                       fmt::format("Delete dir failed: {}", cache_dir.native()));
        LOG(INFO) << "Delete empty dir: " << cache_dir.native();
    }
    return Status::OK();
}

} // namespace io
} // namespace doris
