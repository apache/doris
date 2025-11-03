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

#include "io/fs/file_system.h"

#include "io/fs/file_reader.h"
#include "util/async_io.h" // IWYU pragma: keep

namespace doris {
namespace io {

Status FileSystem::create_file(const Path& file, FileWriterPtr* writer,
                               const FileWriterOptions* opts) {
    Path path;
    RETURN_IF_ERROR(absolute_path(file, path));
    FILESYSTEM_M(create_file_impl(path, writer, opts));
}

Status FileSystem::open_file(const Path& file, FileReaderSPtr* reader,
                             const FileReaderOptions* opts) {
    Path path;
    RETURN_IF_ERROR(absolute_path(file, path));
    FILESYSTEM_M(open_file_impl(path, reader, opts));
}

Status FileSystem::create_directory(const Path& dir, bool failed_if_exists) {
    Path path;
    RETURN_IF_ERROR(absolute_path(dir, path));
    FILESYSTEM_M(create_directory_impl(path, failed_if_exists));
}

Status FileSystem::delete_file(const Path& file) {
    Path path;
    RETURN_IF_ERROR(absolute_path(file, path));
    FILESYSTEM_M(delete_file_impl(path));
}

Status FileSystem::delete_directory(const Path& dir) {
    Path path;
    RETURN_IF_ERROR(absolute_path(dir, path));
    FILESYSTEM_M(delete_directory_impl(path));
}

Status FileSystem::batch_delete(const std::vector<Path>& files) {
    std::vector<Path> abs_files;
    for (auto& file : files) {
        Path abs_file;
        RETURN_IF_ERROR(absolute_path(file, abs_file));
        abs_files.push_back(abs_file);
    }
    FILESYSTEM_M(batch_delete_impl(abs_files));
}

Status FileSystem::exists(const Path& path, bool* res) const {
    Path fs_path;
    RETURN_IF_ERROR(absolute_path(path, fs_path));
    FILESYSTEM_M(exists_impl(fs_path, res));
}

Status FileSystem::file_size(const Path& file, int64_t* file_size) const {
    Path path;
    RETURN_IF_ERROR(absolute_path(file, path));
    FILESYSTEM_M(file_size_impl(path, file_size));
}

Status FileSystem::list(const Path& dir, bool only_file, std::vector<FileInfo>* files,
                        bool* exists) {
    Path path;
    RETURN_IF_ERROR(absolute_path(dir, path));
    FILESYSTEM_M(list_impl(path, only_file, files, exists));
}

Status FileSystem::rename(const Path& orig_name, const Path& new_name) {
    Path orig_path;
    RETURN_IF_ERROR(absolute_path(orig_name, orig_path));
    Path new_path;
    RETURN_IF_ERROR(absolute_path(new_name, new_path));
    FILESYSTEM_M(rename_impl(orig_path, new_path));
}

} // namespace io
} // namespace doris
