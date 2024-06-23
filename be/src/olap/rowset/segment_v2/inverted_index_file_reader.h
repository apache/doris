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
#include <utility>
#include <vector>

#include "common/config.h"
#include "io/fs/file_system.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_file_writer.h"

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

    InvertedIndexFileReader(io::FileSystemSPtr fs, std::string index_path_prefix,
                            InvertedIndexStorageFormatPB storage_format)
            : _fs(std::move(fs)),
              _index_path_prefix(std::move(index_path_prefix)),
              _storage_format(storage_format) {}

    Status init(int32_t read_buffer_size = config::inverted_index_read_buffer_size,
                bool open_idx_file_cache = false);
    Result<std::unique_ptr<DorisCompoundReader>> open(const TabletIndex* index_meta) const;
    void debug_file_entries();
    std::string get_index_file_cache_key(const TabletIndex* index_meta) const;
    std::string get_index_file_path(const TabletIndex* index_meta) const;
    Status index_file_exist(const TabletIndex* index_meta, bool* res) const;
    Status has_null(const TabletIndex* index_meta, bool* res) const;
    Result<InvertedIndexDirectoryMap> get_all_directories();

private:
    Status _init_from_v2(int32_t read_buffer_size);
    Result<std::unique_ptr<DorisCompoundReader>> _open(int64_t index_id,
                                                       const std::string& index_suffix) const;

    IndicesEntriesMap _indices_entries;
    std::unique_ptr<CL_NS(store)::IndexInput> _stream;
    const io::FileSystemSPtr _fs;
    std::string _index_path_prefix;
    int32_t _read_buffer_size = -1;
    bool _open_idx_file_cache = false;
    InvertedIndexStorageFormatPB _storage_format;
    mutable std::shared_mutex _mutex; // Use mutable for const read operations
};

} // namespace segment_v2
} // namespace doris