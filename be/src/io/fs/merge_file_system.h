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

#include <string>
#include <unordered_map>

#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/merge_file_manager.h"

namespace doris::io {

// FileSystem wrapper that handles merge file logic for small files
// When creating a file, it wraps the writer with MergeFileWriter
// When opening a file, it checks if the file is in a merge file and wraps with MergeFileReader
class MergeFileSystem final : public FileSystem {
public:
    MergeFileSystem(FileSystemSPtr inner_fs);

    MergeFileSystem(FileSystemSPtr inner_fs,
                    std::unordered_map<std::string, MergeFileSegmentIndex> index_map);

    ~MergeFileSystem() override = default;

    MergeFileSystem(const MergeFileSystem&) = delete;
    MergeFileSystem& operator=(const MergeFileSystem&) = delete;

protected:
    // Create file and wrap writer with MergeFileWriter
    Status create_file_impl(const Path& file, FileWriterPtr* writer,
                            const FileWriterOptions* opts) override;

    // Open file and wrap reader with MergeFileReader if file is in merge file
    Status open_file_impl(const Path& file, FileReaderSPtr* reader,
                          const FileReaderOptions* opts) override;

    // Other operations not supported for MergeFileSystem
    Status create_directory_impl(const Path& dir, bool failed_if_exists = false) override {
        return Status::InternalError("MergeFileSystem does not support create_directory");
    }

    Status delete_file_impl(const Path& file) override {
        return Status::InternalError("MergeFileSystem does not support delete_file");
    }

    Status batch_delete_impl(const std::vector<Path>& files) override {
        return Status::InternalError("MergeFileSystem does not support batch_delete");
    }

    Status delete_directory_impl(const Path& dir) override {
        return Status::InternalError("MergeFileSystem does not support delete_directory");
    }

    Status exists_impl(const Path& path, bool* res) const override {
        return Status::InternalError("MergeFileSystem does not support exists");
    }

    Status file_size_impl(const Path& file, int64_t* file_size) const override {
        return Status::InternalError("MergeFileSystem does not support file_size");
    }

    Status list_impl(const Path& dir, bool only_file, std::vector<FileInfo>* files,
                     bool* exists) override {
        return Status::InternalError("MergeFileSystem does not support list");
    }

    Status rename_impl(const Path& orig_name, const Path& new_name) override {
        return Status::InternalError("MergeFileSystem does not support rename");
    }

    Status absolute_path(const Path& path, Path& abs_path) const override {
        abs_path = path;
        return Status::OK();
    }

private:
    FileSystemSPtr _inner_fs;
    // Map from small file path to merge file segment index
    std::unordered_map<std::string, MergeFileSegmentIndex> _index_map;
};

} // namespace doris::io
