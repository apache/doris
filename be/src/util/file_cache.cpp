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

#include "util/file_cache.h"

#include "env/env.h"
#include "gutil/strings/substitute.h"

namespace doris {

template <class FileType>
FileCache<FileType>::FileCache(const std::string& cache_name, int max_open_files)
        : _cache_name(cache_name),
          _cache(new_lru_cache(std::string("FileBlockManagerCache:") + cache_name, max_open_files)),
          _is_cache_own(true) {}

template <class FileType>
FileCache<FileType>::FileCache(const std::string& cache_name, std::shared_ptr<Cache> cache)
        : _cache_name(cache_name), _cache(cache), _is_cache_own(false) {}

template <class FileType>
bool FileCache<FileType>::lookup(const std::string& file_name,
                                 OpenedFileHandle<FileType>* file_handle) {
    DCHECK(_cache != nullptr);
    CacheKey key(file_name);
    auto lru_handle = _cache->lookup(key);
    if (lru_handle == nullptr) {
        return false;
    }
    *file_handle = OpenedFileHandle<FileType>(_cache.get(), lru_handle);
    return true;
}

template <class FileType>
void FileCache<FileType>::insert(const std::string& file_name, FileType* file,
                                 OpenedFileHandle<FileType>* file_handle) {
    DCHECK(_cache != nullptr);
    auto deleter = [](const CacheKey& key, void* value) { delete (FileType*)value; };
    CacheKey key(file_name);
    auto lru_handle = _cache->insert(key, file, 1, deleter);
    *file_handle = OpenedFileHandle<FileType>(_cache.get(), lru_handle);
}

// Explicit specialization for callers outside this compilation unit.
template class FileCache<RandomAccessFile>;

} // namespace doris
