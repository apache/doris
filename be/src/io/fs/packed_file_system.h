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
#include "io/fs/packed_file_manager.h"

namespace doris::io {

// FileSystem wrapper that handles packed file logic for small files
// When creating a file, it wraps the writer with PackedFileWriter
// When opening a file, it checks if the file is in a packed file and wraps with PackedFileReader
class PackedFileSystem final : public FileSystem {
public:
    PackedFileSystem(FileSystemSPtr inner_fs, PackedAppendContext append_info = {});

    PackedFileSystem(FileSystemSPtr inner_fs,
                     std::unordered_map<std::string, PackedSliceLocation> index_map,
                     PackedAppendContext append_info = {});

    ~PackedFileSystem() override = default;

    PackedFileSystem(const PackedFileSystem&) = delete;
    PackedFileSystem& operator=(const PackedFileSystem&) = delete;

protected:
    // Create file and wrap writer with PackedFileWriter
    Status create_file_impl(const Path& file, FileWriterPtr* writer,
                            const FileWriterOptions* opts) override;

    // Open file and wrap reader with PackedFileReader if file is in packed file
    Status open_file_impl(const Path& file, FileReaderSPtr* reader,
                          const FileReaderOptions* opts) override;

    // Other operations not supported for PackedFileSystem
    Status create_directory_impl(const Path& dir, bool failed_if_exists = false) override {
        return Status::InternalError("PackedFileSystem does not support create_directory");
    }

    Status delete_file_impl(const Path& file) override {
        return Status::InternalError("PackedFileSystem does not support delete_file");
    }

    Status batch_delete_impl(const std::vector<Path>& files) override {
        return Status::InternalError("PackedFileSystem does not support batch_delete");
    }

    Status delete_directory_impl(const Path& dir) override {
        return Status::InternalError("PackedFileSystem does not support delete_directory");
    }

    Status exists_impl(const Path& path, bool* res) const override;

    Status file_size_impl(const Path& file, int64_t* file_size) const override;

    Status list_impl(const Path& dir, bool only_file, std::vector<FileInfo>* files,
                     bool* exists) override {
        return Status::InternalError("PackedFileSystem does not support list");
    }

    Status rename_impl(const Path& orig_name, const Path& new_name) override {
        return Status::InternalError("PackedFileSystem does not support rename");
    }

    Status absolute_path(const Path& path, Path& abs_path) const override {
        abs_path = path;
        return Status::OK();
    }

private:
    FileSystemSPtr _inner_fs;
    // Map from small file path to packed file slice location
    std::unordered_map<std::string, PackedSliceLocation> _index_map;
    bool _index_map_initialized = false;
    PackedAppendContext _append_info;
};

} // namespace doris::io
