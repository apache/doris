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
#include "io/cache/dummy_file_cache.h"
#include "io/cache/sub_file_cache.h"
#include "io/cache/whole_file_cache.h"
#include "io/fs/local_file_system.h"
#include "olap/storage_engine.h"
#include "util/file_utils.h"
#include "util/string_util.h"

namespace doris {
namespace io {

void GCContextPerDisk::init(const std::string& path, int64_t max_size) {
    _disk_path = path;
    _conf_max_size = max_size;
    _used_size = 0;
}

bool GCContextPerDisk::try_add_file_cache(FileCachePtr cache, int64_t file_size) {
    if (cache->cache_dir().string().substr(0, _disk_path.size()) == _disk_path) {
        _lru_queue.push(cache);
        _used_size += file_size;
        return true;
    }
    return false;
}

void GCContextPerDisk::get_gc_file_caches(std::list<FileCachePtr>& result) {
    while (!_lru_queue.empty() && _used_size > _conf_max_size) {
        auto file_cache = _lru_queue.top();
        _used_size -= file_cache->cache_file_size();
        result.push_back(file_cache);
        _lru_queue.pop();
    }
}

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

void FileCacheManager::_add_file_cache_for_gc_by_disk(std::vector<GCContextPerDisk>& contexts,
                                                      FileCachePtr file_cache) {
    // sort file cache by last match time
    if (config::file_cache_max_size_per_disk > 0) {
        auto file_size = file_cache->cache_file_size();
        if (file_size <= 0) {
            return;
        }
        for (size_t i = 0; i < contexts.size(); ++i) {
            if (contexts[i].try_add_file_cache(file_cache, file_size)) {
                break;
            }
        }
    }
}

void FileCacheManager::_gc_unused_file_caches(std::list<FileCachePtr>& result) {
    std::vector<TabletSharedPtr> tablets =
            StorageEngine::instance()->tablet_manager()->get_all_tablet();
    for (const auto& tablet : tablets) {
        std::vector<Path> seg_file_paths;
        if (io::global_local_filesystem()->list(tablet->tablet_path(), &seg_file_paths).ok()) {
            for (Path seg_file : seg_file_paths) {
                std::string seg_filename = seg_file.native();
                // check if it is a dir name
                if (ends_with(seg_filename, ".dat")) {
                    continue;
                }
                // skip file cache already in memory
                std::stringstream ss;
                ss << tablet->tablet_path() << "/" << seg_filename;
                std::string cache_path = ss.str();

                std::shared_lock<std::shared_mutex> rdlock(_cache_map_lock);
                if (_file_cache_map.find(cache_path) != _file_cache_map.end()) {
                    continue;
                }
                auto file_cache = std::make_shared<DummyFileCache>(
                        cache_path, config::file_cache_alive_time_sec);
                // load cache meta from disk and clean unfinished cache files
                file_cache->load_and_clean();
                // policy1: GC file cache by timeout
                file_cache->clean_timeout_cache();

                result.push_back(file_cache);
            }
        }
    }
}

void FileCacheManager::gc_file_caches() {
    int64_t gc_conf_size = config::file_cache_max_size_per_disk;
    std::vector<GCContextPerDisk> contexts;
    // init for GC by disk size
    if (gc_conf_size > 0) {
        std::vector<DataDir*> data_dirs = doris::StorageEngine::instance()->get_stores();
        contexts.resize(data_dirs.size());
        for (size_t i = 0; i < contexts.size(); ++i) {
            contexts[i].init(data_dirs[i]->path(), gc_conf_size);
        }
    }

    // process unused file caches
    std::list<FileCachePtr> dummy_file_list;
    _gc_unused_file_caches(dummy_file_list);

    {
        std::shared_lock<std::shared_mutex> rdlock(_cache_map_lock);
        for (auto item : dummy_file_list) {
            // check again after _cache_map_lock hold
            if (_file_cache_map.find(item->cache_dir().native()) != _file_cache_map.end()) {
                continue;
            }
            // sort file cache by last match time
            _add_file_cache_for_gc_by_disk(contexts, item);
        }

        // process file caches in memory
        for (std::map<std::string, FileCachePtr>::const_iterator iter = _file_cache_map.cbegin();
             iter != _file_cache_map.cend(); ++iter) {
            if (iter->second == nullptr) {
                continue;
            }
            // policy1: GC file cache by timeout
            iter->second->clean_timeout_cache();
            // sort file cache by last match time
            _add_file_cache_for_gc_by_disk(contexts, iter->second);
        }
    }

    // policy2: GC file cache by disk size
    if (gc_conf_size > 0) {
        for (size_t i = 0; i < contexts.size(); ++i) {
            std::list<FileCachePtr> gc_file_list;
            contexts[i].get_gc_file_caches(gc_file_list);
            for (auto item : gc_file_list) {
                std::shared_lock<std::shared_mutex> rdlock(_cache_map_lock);
                // for dummy file cache, check already used or not again
                if (item->is_dummy_file_cache() &&
                    _file_cache_map.find(item->cache_dir().native()) != _file_cache_map.end()) {
                    continue;
                }
                item->clean_all_cache();
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
