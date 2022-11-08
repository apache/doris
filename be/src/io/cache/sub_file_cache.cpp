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

#include "io/cache/sub_file_cache.h"

#include "common/config.h"
#include "io/fs/local_file_system.h"

namespace doris {
namespace io {

using std::vector;

const static std::string SUB_FILE_CACHE_PREFIX = "SUB_CACHE";

SubFileCache::SubFileCache(const Path& cache_dir, int64_t alive_time_sec,
                           io::FileReaderSPtr remote_file_reader)
        : _cache_dir(cache_dir),
          _alive_time_sec(alive_time_sec),
          _remote_file_reader(remote_file_reader) {}

SubFileCache::~SubFileCache() {}

Status SubFileCache::read_at(size_t offset, Slice result, size_t* bytes_read) {
    std::vector<size_t> need_cache_offsets;
    RETURN_IF_ERROR(_get_need_cache_offsets(offset, result.size, &need_cache_offsets));
    bool need_download = false;
    {
        std::shared_lock<std::shared_mutex> rlock(_cache_map_lock);
        for (vector<size_t>::const_iterator iter = need_cache_offsets.cbegin();
             iter != need_cache_offsets.cend(); ++iter) {
            if (_cache_file_readers.find(*iter) == _cache_file_readers.end() ||
                _cache_file_readers[*iter] == nullptr) {
                need_download = true;
                break;
            }
        }
    }
    if (need_download) {
        std::unique_lock<std::shared_mutex> wrlock(_cache_map_lock);
        bool cache_dir_exist = false;
        RETURN_NOT_OK_STATUS_WITH_WARN(
                io::global_local_filesystem()->exists(_cache_dir, &cache_dir_exist),
                fmt::format("Check local cache dir exist failed. {}", _cache_dir.native()));
        if (!cache_dir_exist) {
            RETURN_NOT_OK_STATUS_WITH_WARN(
                    io::global_local_filesystem()->create_directory(_cache_dir),
                    fmt::format("Create local cache dir failed. {}", _cache_dir.native()));
        }
        for (vector<size_t>::const_iterator iter = need_cache_offsets.cbegin();
             iter != need_cache_offsets.cend(); ++iter) {
            if (_cache_file_readers.find(*iter) == _cache_file_readers.end() ||
                _cache_file_readers[*iter] == nullptr) {
                size_t offset_begin = *iter;
                size_t req_size = config::max_sub_cache_file_size;
                if (offset_begin + req_size > _remote_file_reader->size()) {
                    req_size = _remote_file_reader->size() - offset_begin;
                }
                RETURN_IF_ERROR(_generate_cache_reader(offset_begin, req_size));
            }
        }
        _cache_file_size = _calc_cache_file_size();
    }
    {
        std::shared_lock<std::shared_mutex> rlock(_cache_map_lock);
        *bytes_read = 0;
        for (vector<size_t>::const_iterator iter = need_cache_offsets.cbegin();
             iter != need_cache_offsets.cend(); ++iter) {
            size_t offset_begin = *iter;
            if (_cache_file_readers.find(*iter) == _cache_file_readers.end()) {
                LOG(ERROR) << "Local cache file reader can't be found: " << offset_begin << ", "
                           << offset_begin;
                return Status::OLAPInternalError(OLAP_ERR_OS_ERROR);
            }
            if (offset_begin < offset) {
                offset_begin = offset;
            }
            size_t req_size = *iter + config::max_sub_cache_file_size - offset_begin;
            if (offset + result.size < *iter + config::max_sub_cache_file_size) {
                req_size = offset + result.size - offset_begin;
            }
            Slice read_slice(result.mutable_data() + offset_begin - offset, req_size);
            size_t sub_bytes_read = -1;
            RETURN_NOT_OK_STATUS_WITH_WARN(
                    _cache_file_readers[*iter]->read_at(offset_begin - *iter, read_slice,
                                                        &sub_bytes_read),
                    fmt::format("Read local cache file failed: {}",
                                _cache_file_readers[*iter]->path().native()));
            if (sub_bytes_read != read_slice.size) {
                LOG(ERROR) << "read local cache file failed: "
                           << _cache_file_readers[*iter]->path().native()
                           << ", bytes read: " << sub_bytes_read << " vs req size: " << req_size;
                return Status::OLAPInternalError(OLAP_ERR_OS_ERROR);
            }
            *bytes_read += sub_bytes_read;
            _last_match_times[*iter] = time(nullptr);
        }
    }
    update_last_match_time();
    return Status::OK();
}

Status SubFileCache::_generate_cache_reader(size_t offset, size_t req_size) {
    Path cache_file = _cache_dir / fmt::format("{}_{}", SUB_FILE_CACHE_PREFIX, offset);
    Path cache_done_file = _cache_dir / fmt::format("{}_{}{}", SUB_FILE_CACHE_PREFIX, offset,
                                                    CACHE_DONE_FILE_SUFFIX);
    bool done_file_exist = false;
    RETURN_NOT_OK_STATUS_WITH_WARN(
            io::global_local_filesystem()->exists(cache_done_file, &done_file_exist),
            fmt::format("Check local cache done file exist failed. {}", cache_done_file.native()));

    std::promise<Status> download_st;
    std::future<Status> future = download_st.get_future();
    if (!done_file_exist) {
        ThreadPoolToken* thread_token =
                ExecEnv::GetInstance()->get_serial_download_cache_thread_token();
        if (thread_token != nullptr) {
            auto st = thread_token->submit_func([this, &download_st, cache_done_file, cache_file,
                                                 offset, req_size] {
                auto func = [this, cache_done_file, cache_file, offset, req_size] {
                    bool done_file_exist = false;
                    // Judge again whether cache_done_file exists, it is possible that the cache
                    // is downloaded while waiting in the thread pool
                    RETURN_NOT_OK_STATUS_WITH_WARN(io::global_local_filesystem()->exists(
                                                           cache_done_file, &done_file_exist),
                                                   "Check local cache done file exist failed.");
                    bool cache_file_exist = false;
                    RETURN_NOT_OK_STATUS_WITH_WARN(
                            io::global_local_filesystem()->exists(cache_file, &cache_file_exist),
                            fmt::format("Check local cache file exist failed. {}",
                                        cache_file.native()));
                    if (done_file_exist && cache_file_exist) {
                        return Status::OK();
                    } else if (!done_file_exist && cache_file_exist) {
                        RETURN_NOT_OK_STATUS_WITH_WARN(
                                io::global_local_filesystem()->delete_file(cache_file),
                                fmt::format("Check local cache file exist failed. {}",
                                            cache_file.native()));
                    }
                    RETURN_NOT_OK_STATUS_WITH_WARN(
                            download_cache_to_local(cache_file, cache_done_file,
                                                    _remote_file_reader, req_size, offset),
                            "Download cache from remote to local failed.");
                    return Status::OK();
                };
                download_st.set_value(func());
            });
            if (!st.ok()) {
                LOG(FATAL) << "Failed to submit download cache task to thread pool! "
                           << st.get_error_msg();
            }
        } else {
            return Status::InternalError("Failed to get download cache thread token");
        }
        auto st = future.get();
        if (!st.ok()) {
            return st;
        }
    }
    io::FileReaderSPtr cache_reader;
    RETURN_IF_ERROR(io::global_local_filesystem()->open_file(cache_file, &cache_reader));
    _cache_file_readers.emplace(offset, cache_reader);
    _last_match_times.emplace(offset, time(nullptr));
    update_last_match_time();
    LOG(INFO) << "Create cache file from remote file successfully: "
              << _remote_file_reader->path().native() << "(" << offset << ", " << req_size
              << ") -> " << cache_file.native();
    return Status::OK();
}

Status SubFileCache::_get_need_cache_offsets(size_t offset, size_t req_size,
                                             std::vector<size_t>* cache_offsets) {
    size_t first_offset_begin =
            offset / config::max_sub_cache_file_size * config::max_sub_cache_file_size;
    for (size_t begin = first_offset_begin; begin < offset + req_size;
         begin += config::max_sub_cache_file_size) {
        cache_offsets->push_back(begin);
    }
    return Status::OK();
}

Status SubFileCache::clean_timeout_cache() {
    std::vector<size_t> timeout_keys;
    {
        std::shared_lock<std::shared_mutex> rlock(_cache_map_lock);
        for (std::map<size_t, int64_t>::const_iterator iter = _last_match_times.cbegin();
             iter != _last_match_times.cend(); ++iter) {
            if (time(nullptr) - iter->second > _alive_time_sec) {
                timeout_keys.emplace_back(iter->first);
            }
        }
    }
    if (timeout_keys.size() > 0) {
        std::unique_lock<std::shared_mutex> wrlock(_cache_map_lock);
        for (std::vector<size_t>::const_iterator iter = timeout_keys.cbegin();
             iter != timeout_keys.cend(); ++iter) {
            RETURN_IF_ERROR(_clean_cache_internal(*iter));
        }
        _cache_file_size = _calc_cache_file_size();
    }
    return Status::OK();
}

Status SubFileCache::clean_all_cache() {
    std::unique_lock<std::shared_mutex> wrlock(_cache_map_lock);
    for (std::map<size_t, int64_t>::const_iterator iter = _last_match_times.cbegin();
         iter != _last_match_times.cend(); ++iter) {
        RETURN_IF_ERROR(_clean_cache_internal(iter->first));
    }
    _cache_file_size = _calc_cache_file_size();
    return Status::OK();
}

Status SubFileCache::_clean_cache_internal(size_t offset) {
    if (_cache_file_readers.find(offset) != _cache_file_readers.end()) {
        _cache_file_readers.erase(offset);
    }
    _cache_file_size = 0;
    Path cache_file = _cache_dir / fmt::format("{}_{}", SUB_FILE_CACHE_PREFIX, offset);
    Path done_file = _cache_dir /
                     fmt::format("{}_{}{}", SUB_FILE_CACHE_PREFIX, offset, CACHE_DONE_FILE_SUFFIX);
    bool done_file_exist = false;
    RETURN_NOT_OK_STATUS_WITH_WARN(
            io::global_local_filesystem()->exists(done_file, &done_file_exist),
            "Check local done file exist failed.");
    if (done_file_exist) {
        RETURN_NOT_OK_STATUS_WITH_WARN(
                io::global_local_filesystem()->delete_file(done_file),
                fmt::format("Delete local done file failed: {}", done_file.native()));
    }
    bool cache_file_exist = false;
    RETURN_NOT_OK_STATUS_WITH_WARN(
            io::global_local_filesystem()->exists(cache_file, &cache_file_exist),
            "Check local cache file exist failed.");
    if (cache_file_exist) {
        RETURN_NOT_OK_STATUS_WITH_WARN(
                io::global_local_filesystem()->delete_file(cache_file),
                fmt::format("Delete local cache file failed: {}", cache_file.native()));
    }
    LOG(INFO) << "Delete local cache file successfully: " << cache_file.native();
    return Status::OK();
}

size_t SubFileCache::_calc_cache_file_size() {
    size_t cache_file_size = 0;
    for (std::map<size_t, io::FileReaderSPtr>::const_iterator iter = _cache_file_readers.cbegin();
         iter != _cache_file_readers.cend(); ++iter) {
        cache_file_size += iter->second->size();
    }
    return cache_file_size;
}

} // namespace io
} // namespace doris
