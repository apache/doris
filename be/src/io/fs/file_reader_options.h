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

#include <string>

#include "common/status.h"

namespace doris {
namespace io {

enum class FileCacheType : uint8_t {
    NO_CACHE,
    SUB_FILE_CACHE,
    WHOLE_FILE_CACHE,
    FILE_BLOCK_CACHE,
};

FileCacheType cache_type_from_string(const std::string& type);

// CachePathPolicy it to define which cache path should be used
// for the local cache of the given file(path).
// The dervied class should implement get_cache_path() method
class CachePathPolicy {
public:
    // path: the path of file which will be cached
    // return value: the cache path of the given file.
    virtual std::string get_cache_path(const std::string& path) const { return ""; }
};

class NoCachePathPolicy : public CachePathPolicy {
public:
    NoCachePathPolicy() = default;
    std::string get_cache_path(const std::string& path) const override { return path; }
};

class SegmentCachePathPolicy : public CachePathPolicy {
public:
    SegmentCachePathPolicy() = default;
    std::string get_cache_path(const std::string& path) const override {
        // the segment file path is {rowset_dir}/{schema_hash}/{rowset_id}_{seg_num}.dat
        // cache path is: {rowset_dir}/{schema_hash}/{rowset_id}_{seg_num}/
        return path.substr(0, path.size() - 4) + "/";
    }
};

class FileReaderOptions {
public:
    FileReaderOptions(FileCacheType cache_type_, const CachePathPolicy& path_policy_)
            : cache_type(cache_type_), path_policy(path_policy_) {}

    FileCacheType cache_type;
    CachePathPolicy path_policy;
};

} // namespace io
} // namespace doris
