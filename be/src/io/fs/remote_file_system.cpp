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
#include "io/cache/block/cached_remote_file_reader.h"
#include "io/cache/file_cache_manager.h"
#include "io/fs/file_reader_options.h"
#include "util/async_io.h"

namespace doris {
namespace io {

Status RemoteFileSystem::open_file(const Path& path, const FileReaderOptions& reader_options,
                                   FileReaderSPtr* reader, IOContext* io_ctx) {
    if (bthread_self() == 0) {
        return open_file_impl(path, reader_options, reader, io_ctx);
    }
    Status s;
    auto task = [&] { s = open_file_impl(path, reader_options, reader, io_ctx); };
    AsyncIO::run_task(task, io::FileSystemType::S3);
    return s;
}

Status RemoteFileSystem::open_file_impl(const Path& path, const FileReaderOptions& reader_options,
                                        FileReaderSPtr* reader, IOContext* io_ctx) {
    FileReaderSPtr raw_reader;
    RETURN_IF_ERROR(open_file(path, &raw_reader, io_ctx));
    switch (reader_options.cache_type) {
    case io::FileCachePolicy::NO_CACHE: {
        *reader = raw_reader;
        break;
    }
    case io::FileCachePolicy::SUB_FILE_CACHE:
    case io::FileCachePolicy::WHOLE_FILE_CACHE: {
        std::string cache_path = reader_options.path_policy.get_cache_path(path.native());
        io::FileCachePtr cache_reader = FileCacheManager::instance()->new_file_cache(
                cache_path, config::file_cache_alive_time_sec, raw_reader,
                reader_options.cache_type);
        FileCacheManager::instance()->add_file_cache(cache_path, cache_reader);
        *reader = cache_reader;
        break;
    }
    case io::FileCachePolicy::FILE_BLOCK_CACHE: {
        DCHECK(io_ctx);
        StringPiece str(raw_reader->path().native());
        std::string cache_path = reader_options.path_policy.get_cache_path(path.native());
        *reader =
                std::make_shared<CachedRemoteFileReader>(std::move(raw_reader), cache_path, io_ctx);
        break;
    }
    default: {
        return Status::InternalError("Unknown cache type: {}", reader_options.cache_type);
    }
    }
    return Status::OK();
}

} // namespace io
} // namespace doris
