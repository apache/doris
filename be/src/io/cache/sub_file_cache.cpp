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

#include <glog/logging.h>
#include <sys/types.h>

#include <algorithm>
#include <cstdlib>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "olap/iterators.h"
#include "util/file_utils.h"
#include "util/string_util.h"

namespace doris {
using namespace ErrorCode;
namespace io {

using std::vector;

const static std::string SUB_FILE_CACHE_PREFIX = "SUB_CACHE";

SubFileCache::SubFileCache(const Path& cache_dir, int64_t alive_time_sec,
                           io::FileReaderSPtr remote_file_reader)
        : _cache_dir(cache_dir),
          _alive_time_sec(alive_time_sec),
          _remote_file_reader(remote_file_reader) {}

SubFileCache::~SubFileCache() {}

Status SubFileCache::read_at(size_t offset, Slice result, const IOContext& io_ctx,
                             size_t* bytes_read) {
    _init();
    if (io_ctx.reader_type != READER_QUERY) {
        return _remote_file_reader->read_at(offset, result, io_ctx, bytes_read);
    }
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
                return Status::Error<OS_ERROR>();
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
                    _cache_file_readers[*iter]->read_at(offset_begin - *iter, read_slice, io_ctx,
                                                        &sub_bytes_read),
                    fmt::format("Read local cache file failed: {}",
                                _cache_file_readers[*iter]->path().native()));
            if (sub_bytes_read != read_slice.size) {
                LOG(ERROR) << "read local cache file failed: "
                           << _cache_file_readers[*iter]->path().native()
                           << ", bytes read: " << sub_bytes_read << " vs req size: " << req_size;
                return Status::Error<OS_ERROR>();
            }
            *bytes_read += sub_bytes_read;
            _last_match_times[*iter] = time(nullptr);
        }
    }
    return Status::OK();
}

std::pair<Path, Path> SubFileCache::_cache_path(size_t offset) {
    return {_cache_dir / fmt::format("{}_{}", SUB_FILE_CACHE_PREFIX, offset),
            _cache_dir /
                    fmt::format("{}_{}{}", SUB_FILE_CACHE_PREFIX, offset, CACHE_DONE_FILE_SUFFIX)};
}

Status SubFileCache::_generate_cache_reader(size_t offset, size_t req_size) {
    auto [cache_file, cache_done_file] = _cache_path(offset);
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
            auto st = thread_token->submit_func([this, &download_st,
                                                 cache_done_file = cache_done_file,
                                                 cache_file = cache_file, offset, req_size] {
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
                LOG(FATAL) << "Failed to submit download cache task to thread pool! " << st;
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
    _init();
    SubGcQueue gc_queue;
    _gc_lru_queue.swap(gc_queue);
    std::vector<size_t> timeout_keys;
    {
        std::shared_lock<std::shared_mutex> rlock(_cache_map_lock);
        for (std::map<size_t, int64_t>::const_iterator iter = _last_match_times.cbegin();
             iter != _last_match_times.cend(); ++iter) {
            if (time(nullptr) - iter->second > _alive_time_sec) {
                timeout_keys.emplace_back(iter->first);
            } else {
                _gc_lru_queue.push({iter->first, iter->second});
            }
        }
    }

    std::unique_lock<std::shared_mutex> wrlock(_cache_map_lock);
    if (timeout_keys.size() > 0) {
        for (std::vector<size_t>::const_iterator iter = timeout_keys.cbegin();
             iter != timeout_keys.cend(); ++iter) {
            size_t cleaned_size = 0;
            RETURN_IF_ERROR(_clean_cache_internal(*iter, &cleaned_size));
            _cache_file_size -= cleaned_size;
        }
    }
    return _check_and_delete_empty_dir(_cache_dir);
}

Status SubFileCache::clean_all_cache() {
    std::unique_lock<std::shared_mutex> wrlock(_cache_map_lock);
    for (std::map<size_t, int64_t>::const_iterator iter = _last_match_times.cbegin();
         iter != _last_match_times.cend(); ++iter) {
        RETURN_IF_ERROR(_clean_cache_internal(iter->first, nullptr));
    }
    _cache_file_size = 0;
    return _check_and_delete_empty_dir(_cache_dir);
}

Status SubFileCache::clean_one_cache(size_t* cleaned_size) {
    if (!_gc_lru_queue.empty()) {
        const auto& cache = _gc_lru_queue.top();
        {
            std::unique_lock<std::shared_mutex> wrlock(_cache_map_lock);
            if (auto it = _last_match_times.find(cache.offset);
                it != _last_match_times.end() && it->second == cache.last_match_time) {
                RETURN_IF_ERROR(_clean_cache_internal(cache.offset, cleaned_size));
                _cache_file_size -= *cleaned_size;
                _gc_lru_queue.pop();
            }
        }
        decltype(_last_match_times.begin()) it;
        while (!_gc_lru_queue.empty() &&
               (it = _last_match_times.find(_gc_lru_queue.top().offset)) !=
                       _last_match_times.end() &&
               it->second != _gc_lru_queue.top().last_match_time) {
            _gc_lru_queue.pop();
        }
    }
    if (_gc_lru_queue.empty()) {
        std::unique_lock<std::shared_mutex> wrlock(_cache_map_lock);
        RETURN_IF_ERROR(_check_and_delete_empty_dir(_cache_dir));
    }
    return Status::OK();
}

Status SubFileCache::_clean_cache_internal(size_t offset, size_t* cleaned_size) {
    if (_cache_file_readers.find(offset) != _cache_file_readers.end()) {
        _cache_file_readers.erase(offset);
    }
    if (_last_match_times.find(offset) != _last_match_times.end()) {
        _last_match_times.erase(offset);
    }
    auto [cache_file, done_file] = _cache_path(offset);
    return _remove_cache_and_done(cache_file, done_file, cleaned_size);
}

void SubFileCache::_init() {
    auto init = [this] {
        std::vector<Path> cache_names;

        std::unique_lock<std::shared_mutex> wrlock(_cache_map_lock);
        if (!_get_dir_files_and_remove_unfinished(_cache_dir, cache_names).ok()) {
            return;
        }
        for (const auto& file : cache_names) {
            auto str_vec = split(file.native(), "_");
            size_t offset = std::strtoul(str_vec[str_vec.size() - 1].c_str(), nullptr, 10);

            size_t file_size = 0;
            auto path = _cache_dir / file;
            if (io::global_local_filesystem()->file_size(path, &file_size).ok()) {
                _last_match_times[offset] = time(nullptr);
                _cache_file_size += file_size;
            } else {
                LOG(WARNING) << "get local cache file size failed:" << path.native();
                _clean_cache_internal(offset, nullptr);
            }
        }
    };

    std::call_once(init_flag, init);
}

} // namespace io
} // namespace doris
