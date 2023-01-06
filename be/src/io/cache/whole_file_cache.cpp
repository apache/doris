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

#include "io/cache/whole_file_cache.h"

#include "io/fs/local_file_system.h"
#include "olap/iterators.h"

namespace doris {
using namespace ErrorCode;
namespace io {

const static std::string WHOLE_FILE_CACHE_NAME = "WHOLE_FILE_CACHE";

WholeFileCache::WholeFileCache(const Path& cache_dir, int64_t alive_time_sec,
                               io::FileReaderSPtr remote_file_reader)
        : _cache_dir(cache_dir),
          _alive_time_sec(alive_time_sec),
          _remote_file_reader(remote_file_reader),
          _cache_file_reader(nullptr) {}

WholeFileCache::~WholeFileCache() {}

Status WholeFileCache::read_at(size_t offset, Slice result, const IOContext& io_ctx,
                               size_t* bytes_read) {
    if (io_ctx.reader_type != READER_QUERY) {
        return _remote_file_reader->read_at(offset, result, io_ctx, bytes_read);
    }
    if (_cache_file_reader == nullptr) {
        RETURN_IF_ERROR(_generate_cache_reader(offset, result.size));
    }
    std::shared_lock<std::shared_mutex> rlock(_cache_lock);
    RETURN_NOT_OK_STATUS_WITH_WARN(
            _cache_file_reader->read_at(offset, result, io_ctx, bytes_read),
            fmt::format("Read local cache file failed: {}", _cache_file_reader->path().native()));
    if (*bytes_read != result.size) {
        LOG(ERROR) << "read cache file failed: " << _cache_file_reader->path().native()
                   << ", bytes read: " << bytes_read << " vs required size: " << result.size;
        return Status::Error<OS_ERROR>();
    }
    update_last_match_time();
    return Status::OK();
}

Status WholeFileCache::_generate_cache_reader(size_t offset, size_t req_size) {
    std::unique_lock<std::shared_mutex> wrlock(_cache_lock);
    Path cache_file = _cache_dir / WHOLE_FILE_CACHE_NAME;
    Path cache_done_file =
            _cache_dir / fmt::format("{}{}", WHOLE_FILE_CACHE_NAME, CACHE_DONE_FILE_SUFFIX);
    bool done_file_exist = false;
    RETURN_NOT_OK_STATUS_WITH_WARN(
            io::global_local_filesystem()->exists(cache_done_file, &done_file_exist),
            "Check local cache done file exist failed.");

    std::promise<Status> download_st;
    std::future<Status> future = download_st.get_future();
    if (!done_file_exist) {
        ThreadPoolToken* thread_token =
                ExecEnv::GetInstance()->get_serial_download_cache_thread_token();
        if (thread_token != nullptr) {
            auto st = thread_token->submit_func([this, &download_st, cache_done_file, cache_file] {
                auto func = [this, cache_done_file, cache_file] {
                    bool done_file_exist = false;
                    bool cache_dir_exist = false;
                    RETURN_NOT_OK_STATUS_WITH_WARN(
                            io::global_local_filesystem()->exists(_cache_dir, &cache_dir_exist),
                            fmt::format("Check local cache dir exist failed. {}",
                                        _cache_dir.native()));
                    if (!cache_dir_exist) {
                        RETURN_NOT_OK_STATUS_WITH_WARN(
                                io::global_local_filesystem()->create_directory(_cache_dir),
                                fmt::format("Create local cache dir failed. {}",
                                            _cache_dir.native()));
                    } else {
                        // Judge again whether cache_done_file exists, it is possible that the cache
                        // is downloaded while waiting in the thread pool
                        RETURN_NOT_OK_STATUS_WITH_WARN(io::global_local_filesystem()->exists(
                                                               cache_done_file, &done_file_exist),
                                                       "Check local cache done file exist failed.");
                    }
                    bool cache_file_exist = false;
                    RETURN_NOT_OK_STATUS_WITH_WARN(
                            io::global_local_filesystem()->exists(cache_file, &cache_file_exist),
                            "Check local cache file exist failed.");
                    if (done_file_exist && cache_file_exist) {
                        return Status::OK();
                    } else if (!done_file_exist && cache_file_exist) {
                        RETURN_NOT_OK_STATUS_WITH_WARN(
                                io::global_local_filesystem()->delete_file(cache_file),
                                fmt::format("Check local cache file exist failed. {}",
                                            cache_file.native()));
                    }
                    size_t req_size = _remote_file_reader->size();
                    RETURN_NOT_OK_STATUS_WITH_WARN(
                            download_cache_to_local(cache_file, cache_done_file,
                                                    _remote_file_reader, req_size),
                            "Download cache from remote to local failed.");
                    return Status::OK();
                };
                download_st.set_value(func());
            });
            if (!st.ok()) {
                LOG(FATAL) << "Failed to submit download cache task to thread pool! " << st;
                return st;
            }
        } else {
            return Status::InternalError("Failed to get download cache thread token");
        }
        auto st = future.get();
        if (!st.ok()) {
            return st;
        }
    }
    RETURN_IF_ERROR(io::global_local_filesystem()->open_file(cache_file, &_cache_file_reader));
    _cache_file_size = _cache_file_reader->size();
    LOG(INFO) << "Create cache file from remote file successfully: "
              << _remote_file_reader->path().native() << " -> " << cache_file.native();
    return Status::OK();
}

Status WholeFileCache::clean_timeout_cache() {
    std::unique_lock<std::shared_mutex> wrlock(_cache_lock);
    _gc_match_time = _last_match_time;
    if (time(nullptr) - _last_match_time > _alive_time_sec) {
        _clean_cache_internal(nullptr);
    }
    return Status::OK();
}

Status WholeFileCache::clean_all_cache() {
    std::unique_lock<std::shared_mutex> wrlock(_cache_lock);
    return _clean_cache_internal(nullptr);
}

Status WholeFileCache::clean_one_cache(size_t* cleaned_size) {
    std::unique_lock<std::shared_mutex> wrlock(_cache_lock);
    if (_gc_match_time == _last_match_time) {
        return _clean_cache_internal(cleaned_size);
    }
    return Status::OK();
}

Status WholeFileCache::_clean_cache_internal(size_t* cleaned_size) {
    _cache_file_reader.reset();
    _cache_file_size = 0;
    Path cache_file = _cache_dir / WHOLE_FILE_CACHE_NAME;
    Path done_file =
            _cache_dir / fmt::format("{}{}", WHOLE_FILE_CACHE_NAME, CACHE_DONE_FILE_SUFFIX);
    RETURN_IF_ERROR(_remove_cache_and_done(cache_file, done_file, cleaned_size));
    return _check_and_delete_empty_dir(_cache_dir);
}

bool WholeFileCache::is_gc_finish() const {
    std::shared_lock<std::shared_mutex> rlock(_cache_lock);
    return _cache_file_reader == nullptr;
}

} // namespace io
} // namespace doris
