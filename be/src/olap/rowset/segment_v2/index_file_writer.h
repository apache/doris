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
#include <gen_cpp/olap_common.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include <string>
#include <utility>

#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "olap/rowset/segment_v2/index_storage_format.h"
#include "olap/rowset/segment_v2/inverted_index_common.h"
#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"
#include "olap/rowset/segment_v2/inverted_index_searcher.h"

namespace doris {
class TabletIndex;

namespace segment_v2 {
class DorisFSDirectory;

using InvertedIndexDirectoryMap =
        std::map<std::pair<int64_t, std::string>, std::shared_ptr<lucene::store::Directory>>;

class IndexFileWriter;
using IndexFileWriterPtr = std::unique_ptr<IndexFileWriter>;

class IndexFileWriter {
public:
    IndexFileWriter(io::FileSystemSPtr fs, std::string index_path_prefix, std::string rowset_id,
                    int64_t seg_id, InvertedIndexStorageFormatPB storage_format,
                    io::FileWriterPtr file_writer = nullptr, bool can_use_ram_dir = true);
    virtual ~IndexFileWriter() = default;

    Result<std::shared_ptr<DorisFSDirectory>> open(const TabletIndex* index_meta);
    Status delete_index(const TabletIndex* index_meta);
    Status initialize(InvertedIndexDirectoryMap& indices_dirs);
    Status add_into_searcher_cache();
    Status close();
    const InvertedIndexFileInfo* get_index_file_info() const {
        DCHECK(_closed) << debug_string();
        return &_file_info;
    }
    int64_t get_index_file_total_size() const {
        DCHECK(_closed) << debug_string();
        return _total_file_size;
    }
    const io::FileSystemSPtr& get_fs() const { return _fs; }
    InvertedIndexStorageFormatPB get_storage_format() const { return _storage_format; }
    void set_file_writer_opts(const io::FileWriterOptions& opts) { _opts = opts; }
    std::string debug_string() const;

private:
    Status _insert_directory_into_map(int64_t index_id, const std::string& index_suffix,
                                      std::shared_ptr<DorisFSDirectory> dir);
    virtual Result<std::unique_ptr<IndexSearcherBuilder>> _construct_index_searcher_builder(
            const DorisCompoundReader* dir);

    // Member variables...
    InvertedIndexDirectoryMap _indices_dirs;
    const io::FileSystemSPtr _fs;
    std::string _index_path_prefix;
    std::string _rowset_id;
    int64_t _seg_id;
    InvertedIndexStorageFormatPB _storage_format;
    std::string _tmp_dir;
    const std::shared_ptr<io::LocalFileSystem>& _local_fs;

    // write to disk or stream
    io::FileWriterPtr _idx_v2_writer = nullptr;
    io::FileWriterOptions _opts;

    // v1: all file size
    // v2: file size
    int64_t _total_file_size = 0;
    InvertedIndexFileInfo _file_info;

    // only once
    bool _closed = false;
    bool _can_use_ram_dir = true;

    IndexStorageFormatPtr _index_storage_format;

    friend class IndexStorageFormatV1;
    friend class IndexStorageFormatV2;
    friend class IndexFileWriterTest;
};

} // namespace segment_v2
} // namespace doris
