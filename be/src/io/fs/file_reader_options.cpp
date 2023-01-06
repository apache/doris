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

#include "io/fs/file_reader_options.h"

namespace doris {
namespace io {

FileCacheType cache_type_from_string(const std::string& type) {
    if (type == "sub_file_cache") {
        return FileCacheType::SUB_FILE_CACHE;
    } else if (type == "whole_file_cache") {
        return FileCacheType::WHOLE_FILE_CACHE;
    } else if (type == "file_block_cache") {
        return FileCacheType::FILE_BLOCK_CACHE;
    } else {
        return FileCacheType::NO_CACHE;
    }
}

} // namespace io
} // namespace doris
