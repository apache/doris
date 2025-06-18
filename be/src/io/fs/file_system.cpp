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

#include <bthread/bthread.h>

#include "io/fs/file_reader.h"

namespace doris {
namespace io {

Status FileSystem::create_file(const Path& file, FileWriterPtr* writer,
                               const FileWriterOptions* opts) {
    DCHECK(bthread_self() == 0) << "FileSystem::create_file should not be called in bthread";
    Path path;
    RETURN_IF_ERROR(absolute_path(file, path));
    return create_file_impl(path, writer, opts);
}

Status FileSystem::open_file(const Path& file, FileReaderSPtr* reader,
                             const FileReaderOptions* opts) {
    DCHECK(bthread_self() == 0) << "FileSystem::open_file should not be called in bthread";
    Path path;
    RETURN_IF_ERROR(absolute_path(file, path));
    return open_file_impl(path, reader, opts);
}

Status FileSystem::create_directory(const Path& dir, bool failed_if_exists) {
    DCHECK(bthread_self() == 0) << "FileSystem::create_directory should not be called in bthread";
    Path path;
    RETURN_IF_ERROR(absolute_path(dir, path));
    return create_directory_impl(path, failed_if_exists);
}

Status FileSystem::delete_file(const Path& file) {
    DCHECK(bthread_self() == 0) << "FileSystem::delete_file should not be called in bthread";
    Path path;
    RETURN_IF_ERROR(absolute_path(file, path));
    return delete_file_impl(path);
}

Status FileSystem::delete_directory(const Path& dir) {
    DCHECK(bthread_self() == 0) << "FileSystem::delete_directory should not be called in bthread";
    Path path;
    RETURN_IF_ERROR(absolute_path(dir, path));
    return delete_directory_impl(path);
}

Status FileSystem::batch_delete(const std::vector<Path>& files) {
    DCHECK(bthread_self() == 0) << "FileSystem::batch_delete should not be called in bthread";
    std::vector<Path> abs_files;
    for (const auto& file : files) {
        Path abs_file;
        RETURN_IF_ERROR(absolute_path(file, abs_file));
        abs_files.push_back(abs_file);
    }
    return batch_delete_impl(abs_files);
}

Status FileSystem::exists(const Path& path, bool* res) const {
    DCHECK(bthread_self() == 0) << "FileSystem::exists should not be called in bthread";
    Path fs_path;
    RETURN_IF_ERROR(absolute_path(path, fs_path));
    return exists_impl(fs_path, res);
}

Status FileSystem::file_size(const Path& file, int64_t* file_size) const {
    DCHECK(bthread_self() == 0) << "FileSystem::file_size should not be called in bthread";
    Path path;
    RETURN_IF_ERROR(absolute_path(file, path));
    return file_size_impl(path, file_size);
}

Status FileSystem::list(const Path& dir, bool only_file, std::vector<FileInfo>* files,
                        bool* exists) {
    DCHECK(bthread_self() == 0) << "FileSystem::list should not be called in bthread";
    Path path;
    RETURN_IF_ERROR(absolute_path(dir, path));
    return list_impl(path, only_file, files, exists);
}

Status FileSystem::rename(const Path& orig_name, const Path& new_name) {
    DCHECK(bthread_self() == 0) << "FileSystem::rename should not be called in bthread";
    Path orig_path;
    RETURN_IF_ERROR(absolute_path(orig_name, orig_path));
    Path new_path;
    RETURN_IF_ERROR(absolute_path(new_name, new_path));
    return rename_impl(orig_path, new_path);
}

} // namespace io
} // namespace doris
