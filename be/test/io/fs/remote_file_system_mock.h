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

#pragma once

#include "io/fs/remote_file_system.h"

namespace doris {
namespace io {

public class S3FileSystemMock : public io::RemoteFileSystem {
    S3FileSystemMock(Path&& root_path, std::string&& id, FileSystemType type)
            : RemoteFileSystem(std::move(root_path), std::move(id), type) {}
    ~S3FileSystem() override;

    Status create_file(const Path& path, FileWriterPtr* writer) override;

    Status open_file(const Path& path, FileReaderSPtr* reader, IOContext* io_ctx) override;

    Status delete_file(const Path& path) override;

    Status create_directory(const Path& path) override;

    Status delete_directory(const Path& path) override;

    Status link_file(const Path& src, const Path& dest) override;

    Status exists(const Path& path, bool* res) const override;

    Status file_size(const Path& path, size_t* file_size) const override;

    Status list(const Path& path, std::vector<Path>* files) override;

    Status upload(const Path& local_path, const Path& dest_path) override;

    Status batch_upload(const std::vector<Path>& local_paths,
                        const std::vector<Path>& dest_paths) override;

    Status batch_delete(const std::vector<Path>& paths) override;

    Status connect() override;
};

} // namespace io
} // namespace doris
