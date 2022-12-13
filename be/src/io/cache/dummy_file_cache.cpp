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

#include "io/cache/dummy_file_cache.h"

#include "gutil/strings/util.h"
#include "io/fs/local_file_system.h"
#include "util/file_utils.h"
#include "util/string_util.h"

namespace doris {
namespace io {

DummyFileCache::DummyFileCache(const Path& cache_dir, int64_t alive_time_sec)
        : _cache_dir(cache_dir), _alive_time_sec(alive_time_sec) {}

DummyFileCache::~DummyFileCache() = default;

void DummyFileCache::_add_file_cache(const Path& data_file) {
    Path cache_file = _cache_dir / data_file;
    size_t file_size = 0;
    time_t m_time = 0;
    if (io::global_local_filesystem()->file_size(cache_file, &file_size).ok() &&
        FileUtils::mtime(cache_file.native(), &m_time).ok()) {
        _gc_lru_queue.push({cache_file, m_time});
        _cache_file_size += file_size;
    } else {
        _unfinished_files.push_back(cache_file);
    }
}

void DummyFileCache::_load() {
    // list all files
    std::vector<Path> cache_file_names;
    if (!io::global_local_filesystem()->list(_cache_dir, &cache_file_names).ok()) {
        return;
    }

    // separate DATA file and DONE file
    std::set<Path> cache_names;
    std::list<Path> done_names;
    for (const auto& cache_file_name : cache_file_names) {
        if (ends_with(cache_file_name.native(), CACHE_DONE_FILE_SUFFIX)) {
            done_names.push_back(cache_file_name);
        } else {
            cache_names.insert(cache_file_name);
        }
    }

    // match DONE file with DATA file
    for (auto iter = done_names.begin(); iter != done_names.end(); ++iter) {
        Path cache_filename = StringReplace(iter->native(), CACHE_DONE_FILE_SUFFIX, "", true);
        if (cache_names.find(cache_filename) != cache_names.end()) {
            cache_names.erase(cache_filename);
            _add_file_cache(cache_filename);
        } else {
            // not data file, but with DONE file
            _unfinished_files.push_back(*iter);
        }
    }
    // data file without DONE file
    for (const auto& file : cache_names) {
        _unfinished_files.push_back(file);
    }
}

Status DummyFileCache::_clean_unfinished_cache() {
    // remove cache file without done file
    for (auto iter = _unfinished_files.begin(); iter != _unfinished_files.end(); ++iter) {
        Path cache_file_path = _cache_dir / *iter;
        LOG(INFO) << "Delete unfinished cache file: " << cache_file_path.native();
        if (!io::global_local_filesystem()->delete_file(cache_file_path).ok()) {
            LOG(ERROR) << "delete_file failed: " << cache_file_path.native();
        }
    }
    std::vector<Path> cache_file_names;
    if (io::global_local_filesystem()->list(_cache_dir, &cache_file_names).ok() &&
        cache_file_names.size() == 0) {
        if (global_local_filesystem()->delete_directory(_cache_dir).ok()) {
            LOG(INFO) << "Delete empty dir: " << _cache_dir.native();
        }
    }
    return Status::OK();
}

Status DummyFileCache::load_and_clean() {
    _load();
    return _clean_unfinished_cache();
}

Status DummyFileCache::clean_timeout_cache() {
    while (!_gc_lru_queue.empty() &&
           time(nullptr) - _gc_lru_queue.top().last_match_time > _alive_time_sec) {
        size_t cleaned_size = 0;
        RETURN_IF_ERROR(_clean_cache_internal(_gc_lru_queue.top().file, &cleaned_size));
        _cache_file_size -= cleaned_size;
        _gc_lru_queue.pop();
    }
    return Status::OK();
}

Status DummyFileCache::clean_all_cache() {
    while (!_gc_lru_queue.empty()) {
        RETURN_IF_ERROR(_clean_cache_internal(_gc_lru_queue.top().file, nullptr));
        _gc_lru_queue.pop();
    }
    _cache_file_size = 0;
    return Status::OK();
}

Status DummyFileCache::clean_one_cache(size_t* cleaned_size) {
    if (!_gc_lru_queue.empty()) {
        const auto& cache = _gc_lru_queue.top();
        RETURN_IF_ERROR(_clean_cache_internal(cache.file, cleaned_size));
        _cache_file_size -= *cleaned_size;
        _gc_lru_queue.pop();
    }
    return Status::OK();
}

Status DummyFileCache::_clean_cache_internal(const Path& cache_file_path, size_t* cleaned_size) {
    Path done_file_path = cache_file_path.native() + CACHE_DONE_FILE_SUFFIX;
    return _remove_file(cache_file_path, done_file_path, cleaned_size);
}

} // namespace io
} // namespace doris
