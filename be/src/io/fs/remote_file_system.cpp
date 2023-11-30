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

#include <glog/logging.h>

#include <algorithm>

#include "common/config.h"
#include "io/cache/block/cached_remote_file_reader.h"
#include "io/fs/file_reader.h"
#include "util/async_io.h" // IWYU pragma: keep

namespace doris {
namespace io {

Status RemoteFileSystem::upload(const Path& local_file, const Path& dest_file) {
    auto dest_path = absolute_path(dest_file);
    FILESYSTEM_M(upload_impl(local_file, dest_path));
}

Status RemoteFileSystem::batch_upload(const std::vector<Path>& local_files,
                                      const std::vector<Path>& remote_files) {
    std::vector<Path> remote_paths;
    for (auto& path : remote_files) {
        remote_paths.push_back(absolute_path(path));
    }
    FILESYSTEM_M(batch_upload_impl(local_files, remote_paths));
}

Status RemoteFileSystem::direct_upload(const Path& remote_file, const std::string& content) {
    auto remote_path = absolute_path(remote_file);
    FILESYSTEM_M(direct_upload_impl(remote_path, content));
}

Status RemoteFileSystem::upload_with_checksum(const Path& local_file, const Path& remote,
                                              const std::string& checksum) {
    auto remote_path = absolute_path(remote);
    FILESYSTEM_M(upload_with_checksum_impl(local_file, remote_path, checksum));
}

Status RemoteFileSystem::download(const Path& remote_file, const Path& local) {
    auto remote_path = absolute_path(remote_file);
    FILESYSTEM_M(download_impl(remote_path, local));
}

Status RemoteFileSystem::direct_download(const Path& remote_file, std::string* content) {
    auto remote_path = absolute_path(remote_file);
    FILESYSTEM_M(direct_download_impl(remote_path, content));
}

Status RemoteFileSystem::connect() {
    FILESYSTEM_M(connect_impl());
}

Status RemoteFileSystem::open_file_impl(const Path& path, FileReaderSPtr* reader,
                                        const FileReaderOptions* opts) {
    FileReaderSPtr raw_reader;
    if (!opts) {
        opts = &FileReaderOptions::DEFAULT;
    }
    RETURN_IF_ERROR(open_file_internal(path, &raw_reader, *opts));
    switch (opts->cache_type) {
    case io::FileCachePolicy::NO_CACHE: {
        *reader = raw_reader;
        break;
    }
    case io::FileCachePolicy::FILE_BLOCK_CACHE: {
        *reader = std::make_shared<CachedRemoteFileReader>(std::move(raw_reader), *opts);
        break;
    }
    default: {
        return Status::InternalError("Unknown cache type: {}", opts->cache_type);
    }
    }
    return Status::OK();
}

} // namespace io
} // namespace doris
