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
#include <vector>

#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"

namespace doris {
class TabletIndex;

namespace segment_v2 {
class DorisFSDirectory;
using InvertedIndexDirectoryMap =
        std::map<std::pair<int64_t, std::string>, std::unique_ptr<lucene::store::Directory>>;

class FileInfo {
public:
    std::string filename;
    int64_t filesize;
};

class InvertedIndexFileWriter {
public:
    InvertedIndexFileWriter(io::FileSystemSPtr fs, std::string index_path_prefix,
                            std::string rowset_id, int64_t seg_id,
                            InvertedIndexStorageFormatPB storage_format,
                            io::FileWriterPtr file_writer = nullptr)
            : _fs(std::move(fs)),
              _index_path_prefix(std::move(index_path_prefix)),
              _rowset_id(std::move(rowset_id)),
              _seg_id(seg_id),
              _storage_format(storage_format),
              _idx_v2_writer(std::move(file_writer)) {}

    Result<DorisFSDirectory*> open(const TabletIndex* index_meta);
    Status delete_index(const TabletIndex* index_meta);
    Status initialize(InvertedIndexDirectoryMap& indices_dirs);
    ~InvertedIndexFileWriter() = default;
    int64_t write_v2();
    int64_t write_v1();
    Status close();
    int64_t headerLength();
    InvertedIndexFileInfo get_index_file_info() const { return _file_info; }
    int64_t get_index_file_total_size() const { return _total_file_size; }
    const io::FileSystemSPtr& get_fs() const { return _fs; }
    void sort_files(std::vector<FileInfo>& file_infos);
    void copyFile(const char* fileName, lucene::store::Directory* dir,
                  lucene::store::IndexOutput* output, uint8_t* buffer, int64_t bufferLength);
    InvertedIndexStorageFormatPB get_storage_format() const { return _storage_format; }

    void set_file_writer_opts(const io::FileWriterOptions& opts) { _opts = opts; }

private:
    InvertedIndexDirectoryMap _indices_dirs;
    const io::FileSystemSPtr _fs;
    std::string _index_path_prefix;
    std::string _rowset_id;
    int64_t _seg_id;
    InvertedIndexStorageFormatPB _storage_format;
    // v1: all file size
    // v2: file size
    int64_t _total_file_size = 0;
    // write to disk or stream
    io::FileWriterPtr _idx_v2_writer;
    io::FileWriterOptions _opts;

    InvertedIndexFileInfo _file_info;
};
} // namespace segment_v2
} // namespace doris
