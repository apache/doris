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

#include "io/fs/packed_file_system.h"

#include <utility>

#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/packed_file_reader.h"
#include "io/fs/packed_file_writer.h"

namespace doris::io {

PackedFileSystem::PackedFileSystem(FileSystemSPtr inner_fs, PackedAppendContext append_info)
        : FileSystem(inner_fs->id(), inner_fs->type()),
          _inner_fs(std::move(inner_fs)),
          _append_info(std::move(append_info)) {
    if (_append_info.resource_id.empty() && _inner_fs != nullptr) {
        _append_info.resource_id = _inner_fs->id();
    }
}

PackedFileSystem::PackedFileSystem(FileSystemSPtr inner_fs,
                                   std::unordered_map<std::string, PackedSliceLocation> index_map,
                                   PackedAppendContext append_info)
        : FileSystem(inner_fs->id(), inner_fs->type()),
          _inner_fs(std::move(inner_fs)),
          _index_map(std::move(index_map)),
          _append_info(std::move(append_info)) {
    if (_append_info.resource_id.empty() && _inner_fs != nullptr) {
        _append_info.resource_id = _inner_fs->id();
    }
    _index_map_initialized = true;
}

Status PackedFileSystem::create_file_impl(const Path& file, FileWriterPtr* writer,
                                          const FileWriterOptions* opts) {
    // Create file using inner file system
    FileWriterPtr inner_writer;
    RETURN_IF_ERROR(_inner_fs->create_file(file, &inner_writer, opts));

    // Wrap with PackedFileWriter
    *writer = std::make_unique<PackedFileWriter>(std::move(inner_writer), file, _append_info);
    return Status::OK();
}

Status PackedFileSystem::open_file_impl(const Path& file, FileReaderSPtr* reader,
                                        const FileReaderOptions* opts) {
    // Check if this file is in a packed file
    std::string file_path = file.native();
    auto it = _index_map.find(file_path);
    bool is_packed_file = (it != _index_map.end());

    if (is_packed_file) {
        // File is in packed file, open packed file and wrap with PackedFileReader
        const auto& index = it->second;
        FileReaderSPtr inner_reader;

        // Create options for opening the packed file
        // Disable cache at this layer - we'll add cache wrapper around PackedFileReader instead
        // This ensures cache key is based on segment path, not packed file path
        FileReaderOptions inner_opts = opts ? *opts : FileReaderOptions();
        inner_opts.file_size = index.packed_file_size;
        inner_opts.cache_type = FileCachePolicy::NO_CACHE;

        VLOG_DEBUG << "open packed file: " << index.packed_file_path << ", file: " << file.native()
                   << ", offset: " << index.offset << ", size: " << index.size
                   << ", packed_file_size: " << index.packed_file_size;
        RETURN_IF_ERROR(
                _inner_fs->open_file(Path(index.packed_file_path), &inner_reader, &inner_opts));

        // Create PackedFileReader with segment path
        // PackedFileReader.path() returns segment path, not packed file path
        auto packed_reader = std::make_shared<PackedFileReader>(std::move(inner_reader), file,
                                                                index.offset, index.size);

        // If cache is requested, wrap PackedFileReader with CachedRemoteFileReader
        // This ensures:
        // 1. Cache key = hash(segment_path.filename()) - matches cleanup key
        // 2. Cache size = segment size - correct boundary
        // 3. Each segment has independent cache entry - no interference during cleanup
        if (opts && opts->cache_type != FileCachePolicy::NO_CACHE) {
            FileReaderOptions cache_opts = *opts;
            cache_opts.file_size = index.size; // Use segment size for cache
            *reader = DORIS_TRY(create_cached_file_reader(packed_reader, cache_opts));
        } else {
            *reader = packed_reader;
        }
    } else {
        RETURN_IF_ERROR(_inner_fs->open_file(file, reader, opts));
    }
    return Status::OK();
}

Status PackedFileSystem::exists_impl(const Path& path, bool* res) const {
    VLOG_DEBUG << "packed file system exist, rowset id " << _append_info.rowset_id;
    if (!_index_map_initialized) {
        return Status::InternalError("PackedFileSystem index map is not initialized");
    }
    const auto it = _index_map.find(path.native());
    if (it != _index_map.end()) {
        return _inner_fs->exists(Path(it->second.packed_file_path), res);
    }
    return _inner_fs->exists(path, res);
}

Status PackedFileSystem::file_size_impl(const Path& file, int64_t* file_size) const {
    if (!_index_map_initialized) {
        return Status::InternalError("PackedFileSystem index map is not initialized");
    }
    const auto it = _index_map.find(file.native());
    if (it != _index_map.end()) {
        *file_size = it->second.size;
        return Status::OK();
    }
    return _inner_fs->file_size(file, file_size);
}

} // namespace doris::io
