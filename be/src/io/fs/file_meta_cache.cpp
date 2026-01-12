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

namespace doris {

std::string FileMetaCache::get_key(const std::string file_name, int64_t modification_time,
                                   int64_t file_size) {
    std::string meta_cache_key;
    meta_cache_key.resize(file_name.size() + sizeof(int64_t));

    memcpy(meta_cache_key.data(), file_name.data(), file_name.size());
    if (modification_time != 0) {
        memcpy(meta_cache_key.data() + file_name.size(), &modification_time, sizeof(int64_t));
    } else {
        memcpy(meta_cache_key.data() + file_name.size(), &file_size, sizeof(int64_t));
    }
    return meta_cache_key;
}

std::string FileMetaCache::get_key(io::FileReaderSPtr file_reader,
                                   const io::FileDescription& _file_description) {
    return FileMetaCache::get_key(
            file_reader->path().native(), _file_description.mtime,
            _file_description.file_size == -1 ? file_reader->size() : _file_description.file_size);
}

} // namespace doris
