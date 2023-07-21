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

#include "io/fs/file_meta_cache.h"

#include "vec/exec/format/parquet/parquet_thrift_util.h"

namespace doris {

Status FileMetaCache::get_parquet_footer(io::FileReaderSPtr file_reader, io::IOContext* io_ctx,
                                         int64_t mtime, size_t* meta_size,
                                         ObjLRUCache::CacheHandle* handle) {
    ObjLRUCache::CacheHandle cache_handle;
    std::string cache_key = file_reader->path().native() + std::to_string(mtime);
    auto hit_cache = _cache.lookup({cache_key}, &cache_handle);
    if (hit_cache) {
        *handle = std::move(cache_handle);
        *meta_size = 0;
    } else {
        vectorized::FileMetaData* meta = nullptr;
        RETURN_IF_ERROR(vectorized::parse_thrift_footer(file_reader, &meta, meta_size, io_ctx));
        _cache.insert({cache_key}, meta, handle);
    }

    return Status::OK();
}

} // namespace doris
