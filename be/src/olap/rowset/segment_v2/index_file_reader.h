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

#include <CLucene.h> // IWYU pragma: keep
#include <CLucene/store/IndexInput.h>
#include <gen_cpp/olap_file.pb.h>

#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "io/fs/file_system.h"
#include "olap/rowset/segment_v2/index_file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"

namespace doris {
class TabletIndex;
namespace segment_v2 {
class ReaderFileEntry;
class DorisCompoundReader;

class IndexFileReader {
public:
    // Modern C++ using std::unordered_map with smart pointers for automatic memory management
    using EntriesType = std::unordered_map<std::string, std::unique_ptr<ReaderFileEntry>>;
    // Map to hold the file entries for each index ID.
    using IndicesEntriesMap =
            std::map<std::pair<int64_t, std::string>, std::unique_ptr<EntriesType>>;

    IndexFileReader(io::FileSystemSPtr fs, std::string index_path_prefix,
                    InvertedIndexStorageFormatPB storage_format,
                    InvertedIndexFileInfo idx_file_info = InvertedIndexFileInfo())
            : _fs(std::move(fs)),
              _index_path_prefix(std::move(index_path_prefix)),
              _storage_format(storage_format),
              _idx_file_info(idx_file_info) {}

    MOCK_FUNCTION Status init(int32_t read_buffer_size = config::inverted_index_read_buffer_size,
                              const io::IOContext* io_ctx = nullptr);
    MOCK_FUNCTION Result<std::unique_ptr<DorisCompoundReader, DirectoryDeleter>> open(
            const TabletIndex* index_meta, const io::IOContext* io_ctx = nullptr) const;
    void debug_file_entries();
    std::string get_index_file_cache_key(const TabletIndex* index_meta) const;
    std::string get_index_file_path(const TabletIndex* index_meta) const;
    Status index_file_exist(const TabletIndex* index_meta, bool* res) const;
    Status has_null(const TabletIndex* index_meta, bool* res) const;
    Result<InvertedIndexDirectoryMap> get_all_directories();
    // open file v2, init _stream
    int64_t get_inverted_file_size() const { return _stream == nullptr ? 0 : _stream->length(); }
    friend IndexFileWriter;

protected:
    Status _init_from(int32_t read_buffer_size, const io::IOContext* io_ctx);
    Result<std::unique_ptr<DorisCompoundReader, DirectoryDeleter>> _open(
            int64_t index_id, const std::string& index_suffix,
            const io::IOContext* io_ctx = nullptr) const;

private:
    IndicesEntriesMap _indices_entries;
    std::unique_ptr<CL_NS(store)::IndexInput> _stream = nullptr;
    const io::FileSystemSPtr _fs;
    std::string _index_path_prefix;
    int32_t _read_buffer_size = -1;
    InvertedIndexStorageFormatPB _storage_format;
    mutable std::shared_mutex _mutex; // Use mutable for const read operations
    bool _inited = false;
    InvertedIndexFileInfo _idx_file_info;
};

} // namespace segment_v2
} // namespace doris