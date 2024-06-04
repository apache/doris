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
#include "io/cache/cached_remote_file_reader.h"
#include "io/fs/file_reader.h"
#include "util/async_io.h" // IWYU pragma: keep

namespace doris::io {

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

Status RemoteFileSystem::download(const Path& remote_file, const Path& local) {
    auto remote_path = absolute_path(remote_file);
    FILESYSTEM_M(download_impl(remote_path, local));
}

Status RemoteFileSystem::open_file_impl(const Path& path, FileReaderSPtr* reader,
                                        const FileReaderOptions* opts) {
    FileReaderSPtr raw_reader;
    if (!opts) {
        opts = &FileReaderOptions::DEFAULT;
    }
    RETURN_IF_ERROR(open_file_internal(path, &raw_reader, *opts));
    *reader = DORIS_TRY(create_cached_file_reader(raw_reader, *opts));
    return Status::OK();
}

} // namespace doris::io
