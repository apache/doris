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

#include "remote_file_system_mock.h"

Status S3FileSystemMock::create_file(const Path& path, FileWriterPtr* writer) {
    return Status::OK();
}

Status S3FileSystemMock::open_file(const Path& path, FileReaderSPtr* reader, IOContext* io_ctx) {
    return Status::OK();
}

Status S3FileSystemMock::delete_file(const Path& path) {
    return Status::OK();
}

Status S3FileSystemMock::create_directory(const Path& path) {
    return Status::OK();
}

Status S3FileSystemMock::delete_directory(const Path& path) {
    return Status::OK();
}

Status S3FileSystemMock::link_file(const Path& src, const Path& dest) {
    return Status::OK();
}

Status S3FileSystemMock::exists(const Path& path, bool* res) {
    return Status::OK();
}

Status S3FileSystemMock::file_size(const Path& path, size_t* file_size) {
    return Status::OK();
}

Status S3FileSystemMock::list(const Path& path, std::vector<Path>* files) {
    return Status::OK();
}

Status S3FileSystemMock::upload(const Path& local_path, const Path& dest_path) {
    return Status::OK();
}

Status S3FileSystemMock::batch_upload(const std::vector<Path>& local_paths,
                                      const std::vector<Path>& dest_paths) {
    return Status::OK();
}

Status S3FileSystemMock::batch_delete(const std::vector<Path>& paths) {
    return Status::OK();
}

Status S3FileSystemMock::connect() {
    return Status::OK();
}
