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

#include "util/async_io.h"

namespace doris {
namespace io {

Status FileSystem::create_file(const Path& file, FileWriterPtr* writer) {
    auto path = absolute_path(file);
    if (bthread_self() == 0) {
        return create_file_impl(path, writer);
    }
    Status s;
    auto task = [&] { s = create_file_impl(path, writer); };
    AsyncIO::run_task(task, _type);
    return s;
}

Status FileSystem::open_file(const Path& file, const FileReaderOptions& reader_options,
                             FileReaderSPtr* reader) {
    auto path = absolute_path(file);
    if (bthread_self() == 0) {
        return open_file_impl(path, reader_options, reader);
    }
    Status s;
    auto task = [&] { s = open_file_impl(path, reader_options, reader); };
    AsyncIO::run_task(task, _type);
    return s;
}

Status FileSystem::create_directory(const Path& dir) {
    auto path = absolute_path(dir);
    if (bthread_self() == 0) {
        return create_directory_impl(path);
    }
    Status s;
    auto task = [&] { s = create_directory_impl(path); };
    AsyncIO::run_task(task, _type);
    return s;
}

Status FileSystem::delete_file(const Path& file) {
    auto path = absolute_path(file);
    if (bthread_self() == 0) {
        return delete_file_impl(path);
    }
    Status s;
    auto task = [&] { s = delete_file_impl(path); };
    AsyncIO::run_task(task, _type);
    return s;
}

Status FileSystem::delete_directory(const Path& dir) {
    auto path = absolute_path(dir);
    if (bthread_self() == 0) {
        return delete_directory_impl(path);
    }
    Status s;
    auto task = [&] { s = delete_directory_impl(path); };
    AsyncIO::run_task(task, _type);
    return s;
}

Status FileSystem::batch_delete(const std::vector<Path>& files) {
    std::vector<Path> abs_files;
    for (auto& file : files) {
        abs_files.push_back(absolute_path(file));
    }
    if (bthread_self() == 0) {
        return batch_delete_impl(abs_files);
    }
    Status s;
    auto task = [&] { s = batch_delete_impl(abs_files); };
    AsyncIO::run_task(task, _type);
    return s;
}

Status FileSystem::exists(const Path& path, bool* res) const {
    auto fs_path = absolute_path(path);
    if (bthread_self() == 0) {
        return exists_impl(fs_path, res);
    }
    Status s;
    auto task = [&] { s = exists_impl(fs_path, res); };
    AsyncIO::run_task(task, _type);
    return s;
}

Status FileSystem::file_size(const Path& file, size_t* file_size) const {
    auto path = absolute_path(file);
    if (bthread_self() == 0) {
        return file_size_impl(path, file_size);
    }
    Status s;
    auto task = [&] { s = file_size_impl(path, file_size); };
    AsyncIO::run_task(task, _type);
    return s;
}

Status FileSystem::list(const Path& dir, bool only_file, std::vector<FileInfo>* files,
                        bool* exists) {
    auto path = absolute_path(dir);
    if (bthread_self() == 0) {
        return list_impl(path, only_file, files, exists);
    }
    Status s;
    auto task = [&] { s = list_impl(path, only_file, files, exists); };
    AsyncIO::run_task(task, _type);
    return s;
}

Status FileSystem::rename(const Path& orig_name, const Path& new_name) {
    auto orig_path = absolute_path(orig_name);
    auto new_path = absolute_path(new_name);
    if (bthread_self() == 0) {
        return rename_impl(orig_path, new_path);
    }
    Status s;
    auto task = [&] { s = rename_impl(orig_path, new_path); };
    AsyncIO::run_task(task, _type);
    return s;
}

Status FileSystem::rename_dir(const Path& orig_name, const Path& new_name) {
    auto orig_path = absolute_path(orig_name);
    auto new_path = absolute_path(new_name);
    if (bthread_self() == 0) {
        return rename_dir_impl(orig_path, new_path);
    }
    Status s;
    auto task = [&] { s = rename_dir_impl(orig_path, new_path); };
    AsyncIO::run_task(task, _type);
    return s;
}

} // namespace io
} // namespace doris
