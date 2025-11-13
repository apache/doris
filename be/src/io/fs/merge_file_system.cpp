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

#include "io/fs/merge_file_system.h"

#include <utility>

#include "common/status.h"
#include "io/fs/merge_file_reader.h"
#include "io/fs/merge_file_writer.h"

namespace doris::io {

MergeFileSystem::MergeFileSystem(FileSystemSPtr inner_fs, MergeFileAppendInfo append_info)
        : FileSystem(inner_fs->id(), inner_fs->type()),
          _inner_fs(std::move(inner_fs)),
          _append_info(std::move(append_info)) {
    if (_append_info.resource_id.empty() && _inner_fs != nullptr) {
        _append_info.resource_id = _inner_fs->id();
    }
}

MergeFileSystem::MergeFileSystem(FileSystemSPtr inner_fs,
                                 std::unordered_map<std::string, MergeFileSegmentIndex> index_map,
                                 MergeFileAppendInfo append_info)
        : FileSystem(inner_fs->id(), inner_fs->type()),
          _inner_fs(std::move(inner_fs)),
          _index_map(std::move(index_map)),
          _append_info(std::move(append_info)) {
    if (_append_info.resource_id.empty() && _inner_fs != nullptr) {
        _append_info.resource_id = _inner_fs->id();
    }
    _index_map_initialized = true;
}

Status MergeFileSystem::create_file_impl(const Path& file, FileWriterPtr* writer,
                                         const FileWriterOptions* opts) {
    // Create file using inner file system
    FileWriterPtr inner_writer;
    RETURN_IF_ERROR(_inner_fs->create_file(file, &inner_writer, opts));

    // Wrap with MergeFileWriter
    *writer = std::make_unique<MergeFileWriter>(std::move(inner_writer), file, _append_info);
    return Status::OK();
}

Status MergeFileSystem::open_file_impl(const Path& file, FileReaderSPtr* reader,
                                       const FileReaderOptions* opts) {
    // Check if this file is in a merge file
    std::string file_path = file.native();
    auto it = _index_map.find(file_path);
    bool is_merge_file = (it != _index_map.end());

    if (is_merge_file) {
        // File is in merge file, open merge file and wrap with MergeFileReader
        const auto& index = it->second;
        FileReaderSPtr inner_reader;
        // Create a new FileReaderOptions with the correct file size
        FileReaderOptions local_opts = opts ? *opts : FileReaderOptions();
        // DCHECK(opts->file_size == -1 || opts->file_size == index.size)
        //         << "file size is not correct, expected: " << index.size
        //         << ", actual: " << opts->file_size;
        // local_opts.file_size = index.size + index.offset;
        local_opts.file_size = -1;
        LOG(INFO) << "open merge file: " << index.merge_file_path << ", file: " << file.native()
                  << ", offset: " << index.offset << ", size: " << index.size;
        RETURN_IF_ERROR(
                _inner_fs->open_file(Path(index.merge_file_path), &inner_reader, &local_opts));

        *reader = std::make_shared<MergeFileReader>(std::move(inner_reader), file, index.offset,
                                                    index.size);
    } else {
        RETURN_IF_ERROR(_inner_fs->open_file(file, reader, opts));
    }
    return Status::OK();
}

Status MergeFileSystem::exists_impl(const Path& path, bool* res) const {
    LOG(INFO) << "merge file system exist, rowset id " << _append_info.rowset_id;
    if (!_index_map_initialized) {
        return Status::InternalError("MergeFileSystem index map is not initialized");
    }
    const auto it = _index_map.find(path.native());
    if (it != _index_map.end()) {
        return _inner_fs->exists(Path(it->second.merge_file_path), res);
    }
    return _inner_fs->exists(path, res);
}

Status MergeFileSystem::file_size_impl(const Path& file, int64_t* file_size) const {
    if (!_index_map_initialized) {
        return Status::InternalError("MergeFileSystem index map is not initialized");
    }
    const auto it = _index_map.find(file.native());
    if (it != _index_map.end()) {
        *file_size = it->second.size;
        return Status::OK();
    }
    return _inner_fs->file_size(file, file_size);
}

} // namespace doris::io
