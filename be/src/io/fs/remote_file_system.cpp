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

#include "io/fs/remote_file_system.h"

#include "gutil/strings/stringpiece.h"
#include "io/cache/file_cache_manager.h"
#include "io/fs/file_reader_options.h"

namespace doris {
namespace io {

Status RemoteFileSystem::open_file(const Path& path, const FileReaderOptions& reader_options,
                                   FileReaderSPtr* reader) {
    FileReaderSPtr raw_reader;
    RETURN_IF_ERROR(open_file(path, &raw_reader));
    switch (reader_options.cache_type) {
    case io::FileCacheType::NO_CACHE: {
        *reader = raw_reader;
        break;
    }
    case io::FileCacheType::SUB_FILE_CACHE:
    case io::FileCacheType::WHOLE_FILE_CACHE: {
        StringPiece str(path.native());
        std::string cache_path = reader_options.path_policy.get_cache_path(str.as_string());
        io::FileCachePtr cache_reader = FileCacheManager::instance()->new_file_cache(
                cache_path, config::file_cache_alive_time_sec, raw_reader,
                reader_options.cache_type);
        FileCacheManager::instance()->add_file_cache(cache_path, cache_reader);
        *reader = cache_reader;
        break;
    }
    case io::FileCacheType::FILE_BLOCK_CACHE: {
        return Status::NotSupported("add file block cache reader");
    }
    default: {
        // TODO: add file block cache reader
        return Status::InternalError("Unknown cache type: {}", reader_options.cache_type);
    }
    }
    return Status::OK();
}

} // namespace io
} // namespace doris
