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
#include <string_view>

#include "io/fs/file_meta_cache.h"

namespace doris {

namespace io {
class BlockFileCache;
struct UInt128Wrapper;
} // namespace io

class FileMetaDiskCache final : public FileMetaPersistentCache {
public:
    explicit FileMetaDiskCache(io::BlockFileCache* cache = nullptr) : _cache(cache) {}

    Status read(FileMetaCacheFormat format, const std::string& key, int64_t modification_time,
                int64_t file_size, std::string* payload) override;

    Status write(FileMetaCacheFormat format, const std::string& key, int64_t modification_time,
                 int64_t file_size, std::string_view payload) override;

    void remove(FileMetaCacheFormat format, const std::string& key) override;

private:
    static std::string get_key(FileMetaCacheFormat format, std::string_view file_meta_cache_key);

    io::BlockFileCache* get_cache(const io::UInt128Wrapper& hash) const;

    io::BlockFileCache* _cache = nullptr;
};

} // namespace doris
