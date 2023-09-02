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

#include "io/fs/file_reader_writer_fwd.h"
#include "util/obj_lru_cache.h"

namespace doris {

// A file meta cache depends on a LRU cache.
// Such as parsed parquet footer.
// The capacity will limit the number of cache entries in cache.
class FileMetaCache {
public:
    FileMetaCache(int64_t capacity) : _cache(capacity) {}

    FileMetaCache(const FileMetaCache&) = delete;
    const FileMetaCache& operator=(const FileMetaCache&) = delete;

    ObjLRUCache& cache() { return _cache; }

    Status get_parquet_footer(io::FileReaderSPtr file_reader, io::IOContext* io_ctx, int64_t mtime,
                              size_t* meta_size, ObjLRUCache::CacheHandle* handle);

    Status get_orc_footer() {
        // TODO: implement
        return Status::OK();
    }

private:
    ObjLRUCache _cache;
};

} // namespace doris
