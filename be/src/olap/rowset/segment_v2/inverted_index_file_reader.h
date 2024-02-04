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

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "io/fs/file_system.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"

namespace doris {
class TabletIndex;
namespace segment_v2 {
class ReaderFileEntry;
class DorisCompoundReader;

class InvertedIndexFileReader {
public:
    using EntriesType =
            lucene::util::CLHashMap<char*, ReaderFileEntry*, lucene::util::Compare::Char,
                                    lucene::util::Equals::Char, lucene::util::Deletor::acArray,
                                    lucene::util::Deletor::Object<ReaderFileEntry>>;
    // Map to hold the file entries for each index ID.
    using IndicesEntriesMap =
            std::map<std::pair<int64_t, std::string>, std::unique_ptr<EntriesType>>;

    InvertedIndexFileReader(const io::FileSystemSPtr& fs, io::Path segment_file_dir,
                            std::string segment_file_name,
                            InvertedIndexStorageFormatPB storage_format)
            : _fs(fs),
              _index_file_dir(std::move(segment_file_dir)),
              _storage_format(storage_format) {
        if (_storage_format == InvertedIndexStorageFormatPB::V1) {
            _index_file_name = std::move(segment_file_name);
        } else {
            _index_file_name = InvertedIndexDescriptor::get_index_file_name(segment_file_name);
        }
    }

    Status init(int32_t read_buffer_size = config::inverted_index_read_buffer_size,
                bool open_idx_file_cache = false);
    Result<std::unique_ptr<DorisCompoundReader>> open(const TabletIndex* index_meta) const;
    void debug_file_entries();
    const io::Path& get_index_file_dir() const { return _index_file_dir; };
    const std::string& get_index_file_name() const { return _index_file_name; };
    const io::FileSystemSPtr& get_fs() const { return _fs; }
    std::string get_index_file_path(const TabletIndex* index_meta) const;
    Status index_file_exist(const TabletIndex* index_meta, bool* res) const;

private:
    Status _init_from_v2(int32_t read_buffer_size);

    IndicesEntriesMap _indices_entries;
    std::unique_ptr<CL_NS(store)::IndexInput> _stream;
    const io::FileSystemSPtr _fs;
    io::Path _index_file_dir;
    std::string _index_file_name;
    int32_t _read_buffer_size = -1;
    bool _open_idx_file_cache = false;
    InvertedIndexStorageFormatPB _storage_format;
};

} // namespace segment_v2
} // namespace doris