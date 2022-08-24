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

#include "io/cache/file_cache_manager.h"

#include "gutil/strings/util.h"
#include "io/cache/sub_file_cache.h"
#include "io/cache/whole_file_cache.h"
#include "io/fs/local_file_system.h"
#include "util/file_utils.h"
#include "util/string_util.h"

namespace doris {
namespace io {

void FileCacheManager::add_file_cache(const std::string& cache_path, FileCachePtr file_cache) {
    std::lock_guard<std::shared_mutex> wrlock(_cache_map_lock);
    _file_cache_map.emplace(cache_path, file_cache);
}

void FileCacheManager::remove_file_cache(const std::string& cache_path) {
    bool cache_path_exist = false;
    {
        std::shared_lock<std::shared_mutex> rdlock(_cache_map_lock);
        if (_file_cache_map.find(cache_path) == _file_cache_map.end()) {
            bool cache_dir_exist = false;
            if (global_local_filesystem()->exists(cache_path, &cache_dir_exist).ok() &&
                cache_dir_exist) {
                Status st = global_local_filesystem()->delete_directory(cache_path);
                if (!st.ok()) {
                    LOG(WARNING) << st.to_string();
                }
            }
        } else {
            cache_path_exist = true;
            _file_cache_map.find(cache_path)->second->clean_all_cache();
        }
    }
    if (cache_path_exist) {
        std::lock_guard<std::shared_mutex> wrlock(_cache_map_lock);
        _file_cache_map.erase(cache_path);
    }
}

void FileCacheManager::clean_timeout_caches() {
    std::shared_lock<std::shared_mutex> rdlock(_cache_map_lock);
    for (std::map<std::string, FileCachePtr>::const_iterator iter = _file_cache_map.cbegin();
         iter != _file_cache_map.cend(); ++iter) {
        if (iter->second == nullptr) {
            continue;
        }
        iter->second->clean_timeout_cache();
    }
}

void FileCacheManager::clean_timeout_file_not_in_mem(const std::string& cache_path) {
    time_t now = time(nullptr);
    std::shared_lock<std::shared_mutex> rdlock(_cache_map_lock);
    // Deal with caches not in _file_cache_map
    if (_file_cache_map.find(cache_path) == _file_cache_map.end()) {
        std::vector<Path> cache_file_names;
        if (io::global_local_filesystem()->list(cache_path, &cache_file_names).ok()) {
            std::map<std::string, bool> cache_names;
            std::list<std::string> done_names;
            for (Path cache_file_name : cache_file_names) {
                std::string filename = cache_file_name.native();
                if (ends_with(filename, CACHE_DONE_FILE_SUFFIX)) {
                    cache_names[filename] = true;
                    continue;
                }
                done_names.push_back(filename);
                std::stringstream done_file_ss;
                done_file_ss << cache_path << "/" << filename;
                std::string done_file_path = done_file_ss.str();
                time_t m_time;
                if (!FileUtils::mtime(done_file_path, &m_time).ok()) {
                    continue;
                }
                if (now - m_time < config::file_cache_alive_time_sec) {
                    continue;
                }
                std::string cache_file_path = StringReplace(done_file_path, CACHE_DONE_FILE_SUFFIX,
                                                            "", true);
                LOG(INFO) << "Delete timeout done_cache_path: " << done_file_path
                          << ", cache_file_path: " << cache_file_path << ", m_time: " << m_time;
                if (!io::global_local_filesystem()->delete_file(done_file_path).ok()) {
                    LOG(ERROR) << "delete_file failed: " << done_file_path;
                    continue;
                }
                if (!io::global_local_filesystem()->delete_file(cache_file_path).ok()) {
                    LOG(ERROR) << "delete_file failed: " << cache_file_path;
                    continue;
                }
            }
            // find cache file without done file.
            for (std::list<std::string>::iterator itr = done_names.begin(); itr != done_names.end();
                 ++itr) {
                std::string cache_filename = StringReplace(*itr, CACHE_DONE_FILE_SUFFIX, "", true);
                if (cache_names.find(cache_filename) != cache_names.end()) {
                    cache_names.erase(cache_filename);
                }
            }
            // remove cache file without done file
            for (std::map<std::string, bool>::iterator itr = cache_names.begin();
                 itr != cache_names.end(); ++itr) {
                std::stringstream cache_file_ss;
                cache_file_ss << cache_path << "/" << itr->first;
                std::string cache_file_path = cache_file_ss.str();
                time_t m_time;
                if (!FileUtils::mtime(cache_file_path, &m_time).ok()) {
                    continue;
                }
                if (now - m_time < config::file_cache_alive_time_sec) {
                    continue;
                }
                LOG(INFO) << "Delete cache file without done file: " << cache_file_path;
                if (!io::global_local_filesystem()->delete_file(cache_file_path).ok()) {
                    LOG(ERROR) << "delete_file failed: " << cache_file_path;
                }
            }
            if (io::global_local_filesystem()->list(cache_path, &cache_file_names).ok() &&
                cache_file_names.size() == 0) {
                if (global_local_filesystem()->delete_directory(cache_path).ok()) {
                    LOG(INFO) << "Delete empty dir: " << cache_path;
                }
            }
        }
    }
}

FileCachePtr FileCacheManager::new_file_cache(const std::string& cache_dir, int64_t alive_time_sec,
                                              io::FileReaderSPtr remote_file_reader,
                                              const std::string& file_cache_type) {
    if (file_cache_type == "whole_file_cache") {
        return std::make_unique<WholeFileCache>(cache_dir, alive_time_sec, remote_file_reader);
    } else if (file_cache_type == "sub_file_cache") {
        return std::make_unique<SubFileCache>(cache_dir, alive_time_sec, remote_file_reader);
    } else {
        return nullptr;
    }
}

bool FileCacheManager::exist(const std::string& cache_path) {
    std::shared_lock<std::shared_mutex> rdlock(_cache_map_lock);
    return _file_cache_map.find(cache_path) != _file_cache_map.end();
}

FileCacheManager* FileCacheManager::instance() {
    static FileCacheManager cache_manager;
    return &cache_manager;
}

} // namespace io
} // namespace doris
