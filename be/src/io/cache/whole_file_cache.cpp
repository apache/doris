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

#include "io/cache/whole_file_cache.h"

namespace doris {
namespace io {

WholeFileCache::WholeFileCache(const Path& cache_file_path, FileReader* remote_file_reader,
                               int64_t alive_time_sec)
        : _cache_file_path(cache_file_path),
          _remote_file_reader(remote_file_reader),
          _alive_time_sec(alive_time_sec) {
}

WholeFileCache::~WholeFileCache() { }

Status WholeFileCache::read_at(size_t offset, Slice result, size_t* bytes_read) {
    return Status::OK();
}

Status WholeFileCache::clean_timeout_cache() {
    return Status::OK();
}

Status WholeFileCache::clean_all_cache() {
    return Status::OK();
}

} // namespace io
} // namespace doris
