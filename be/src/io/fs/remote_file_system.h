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

#include "io/fs/file_system.h"

namespace doris {
namespace io {

class RemoteFileSystem : public FileSystem {
public:
    RemoteFileSystem(Path&& root_path, ResourceId&& resource_id, FileSystemType type)
            : FileSystem(std::move(root_path), std::move(resource_id), type) {}
    ~RemoteFileSystem() override = default;

    // `local_path` should be an absolute path on local filesystem.
    virtual Status upload(const Path& local_path, const Path& dest_path) = 0;

    virtual Status batch_upload(const std::vector<Path>& local_paths,
                                const std::vector<Path>& dest_paths) = 0;

    virtual Status connect() = 0;

    Status open_file(const Path& path, const FileReaderOptions& reader_options,
                     FileReaderSPtr* reader) override;

    Status open_file(const Path& path, FileReaderSPtr* reader) override {
        return Status::NotSupported("implemented in derived classes");
    }
};

} // namespace io
} // namespace doris
