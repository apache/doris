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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"

namespace doris::io {

class RemoteFileSystem : public FileSystem {
public:
    RemoteFileSystem(Path&& root_path, std::string&& id, FileSystemType type)
            : FileSystem(std::move(id), type), _root_path(std::move(root_path)) {}
    ~RemoteFileSystem() override = default;

    Status upload(const Path& local_file, const Path& dest_file);
    Status batch_upload(const std::vector<Path>& local_files,
                        const std::vector<Path>& remote_files);
    Status download(const Path& remote_file, const Path& local);

    // the root path of this fs.
    const Path& root_path() const { return _root_path; }

protected:
    Status open_file_impl(const Path& file, FileReaderSPtr* reader,
                          const FileReaderOptions* opts) override;
    /// upload load_file to remote remote_file
    /// local_file should be an absolute path on local filesystem.
    virtual Status upload_impl(const Path& local_file, const Path& remote_file) = 0;

    /// upload all files in load_files to remote_files
    /// path in local_files should be an absolute path on local filesystem.
    /// the size of local_files and remote_files must be equal.
    virtual Status batch_upload_impl(const std::vector<Path>& local_files,
                                     const std::vector<Path>& remote_files) = 0;

    /// download remote_file to local_file
    /// local_file should be an absolute path on local filesystem.
    virtual Status download_impl(const Path& remote_file, const Path& local_file) = 0;

    // The derived class should implement this method.
    // if file_size < 0, the file size should be fetched from file system
    virtual Status open_file_internal(const Path& file, FileReaderSPtr* reader,
                                      const FileReaderOptions& opts) = 0;

    Path absolute_path(const Path& path) const override {
        if (path.is_absolute()) {
            return path;
        }
        return _root_path / path;
    }

    Path _root_path;
};

using RemoteFileSystemSPtr = std::shared_ptr<RemoteFileSystem>;

} // namespace doris::io
