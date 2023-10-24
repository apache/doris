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

#include "util/async_io.h" // IWYU pragma: keep

namespace doris {
namespace io {

Status FileSystem::create_file(const Path& file, FileWriterPtr* writer,
                               const FileWriterOptions* opts) {
    auto path = absolute_path(file);
    FILESYSTEM_M(create_file_impl(path, writer, opts));
}

Status FileSystem::open_file(const FileDescription& fd, const FileReaderOptions& reader_options,
                             FileReaderSPtr* reader) {
    auto path = absolute_path(fd.path);
    FILESYSTEM_M(open_file_impl(fd, path, reader_options, reader));
}

Status FileSystem::create_directory(const Path& dir, bool failed_if_exists) {
    auto path = absolute_path(dir);
    FILESYSTEM_M(create_directory_impl(path, failed_if_exists));
}

Status FileSystem::delete_file(const Path& file) {
    auto path = absolute_path(file);
    FILESYSTEM_M(delete_file_impl(path));
}

Status FileSystem::delete_directory(const Path& dir) {
    auto path = absolute_path(dir);
    FILESYSTEM_M(delete_directory_impl(path));
}

Status FileSystem::batch_delete(const std::vector<Path>& files) {
    std::vector<Path> abs_files;
    for (auto& file : files) {
        abs_files.push_back(absolute_path(file));
    }
    FILESYSTEM_M(batch_delete_impl(abs_files));
}

Status FileSystem::exists(const Path& path, bool* res) const {
    auto fs_path = absolute_path(path);
    FILESYSTEM_M(exists_impl(fs_path, res));
}

Status FileSystem::file_size(const Path& file, int64_t* file_size) const {
    auto path = absolute_path(file);
    FILESYSTEM_M(file_size_impl(path, file_size));
}

Status FileSystem::list(const Path& dir, bool only_file, std::vector<FileInfo>* files,
                        bool* exists) {
    auto path = absolute_path(dir);
    FILESYSTEM_M(list_impl(path, only_file, files, exists));
}

Status FileSystem::rename(const Path& orig_name, const Path& new_name) {
    auto orig_path = absolute_path(orig_name);
    auto new_path = absolute_path(new_name);
    FILESYSTEM_M(rename_impl(orig_path, new_path));
}

Status FileSystem::rename_dir(const Path& orig_name, const Path& new_name) {
    auto orig_path = absolute_path(orig_name);
    auto new_path = absolute_path(new_name);
    FILESYSTEM_M(rename_dir_impl(orig_path, new_path));
}

} // namespace io
} // namespace doris
