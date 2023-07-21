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

#pragma once

#include <stdint.h>

#include <string>

namespace doris {
namespace io {

enum class FileCachePolicy : uint8_t {
    NO_CACHE,
    SUB_FILE_CACHE,
    WHOLE_FILE_CACHE,
    FILE_BLOCK_CACHE,
};

FileCachePolicy cache_type_from_string(const std::string& type);

// CachePathPolicy it to define which cache path should be used
// for the local cache of the given file(path).
// The dervied class should implement get_cache_path() method
class CachePathPolicy {
public:
    virtual ~CachePathPolicy() = default;
    // path: the path of file which will be cached
    // return value: the cache path of the given file.
    virtual std::string get_cache_path(const std::string& path) const = 0;
};

class NoCachePathPolicy : public CachePathPolicy {
public:
    std::string get_cache_path(const std::string& path) const override { return ""; }
};

class SegmentCachePathPolicy : public CachePathPolicy {
public:
    void set_cache_path(const std::string& cache_path) { _cache_path = cache_path; }

    std::string get_cache_path(const std::string& path) const override { return _cache_path; }

private:
    std::string _cache_path;
};

class FileBlockCachePathPolicy : public CachePathPolicy {
public:
    std::string get_cache_path(const std::string& path) const override { return path; }
};

class FileReaderOptions {
public:
    FileReaderOptions(FileCachePolicy cache_type_, const CachePathPolicy& path_policy_)
            : cache_type(cache_type_), path_policy(path_policy_) {}

    FileCachePolicy cache_type;
    const CachePathPolicy& path_policy;
    bool has_cache_base_path = false;
    std::string cache_base_path;
    // Use modification time to determine whether the file is changed
    int64_t modification_time = 0;

    void specify_cache_path(const std::string& base_path) {
        has_cache_base_path = true;
        cache_base_path = base_path;
    }

    static FileReaderOptions DEFAULT;
};

} // namespace io
} // namespace doris
