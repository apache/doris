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
#include "io/fs/local_file_system.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "runtime/exec_env.h"

namespace doris {
class TabletIndex;

namespace segment_v2 {
class DorisFSDirectory;
using InvertedIndexDirectoryMap =
        std::map<std::pair<int64_t, std::string>, std::shared_ptr<lucene::store::Directory>>;

class InvertedIndexFileWriter;
using InvertedIndexFileWriterPtr = std::unique_ptr<InvertedIndexFileWriter>;

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
              _local_fs(io::global_local_filesystem()),
              _idx_v2_writer(std::move(file_writer)) {
        auto tmp_file_dir = ExecEnv::GetInstance()->get_tmp_file_dirs()->get_tmp_file_dir();
        _tmp_dir = tmp_file_dir.native();
    }

    Result<std::shared_ptr<DorisFSDirectory>> open(const TabletIndex* index_meta);
    Status delete_index(const TabletIndex* index_meta);
    Status initialize(InvertedIndexDirectoryMap& indices_dirs);
    virtual ~InvertedIndexFileWriter() = default;
    Status write_v2();
    Status write_v1();
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

    std::string debug_string() const {
        std::stringstream indices_dirs;
        for (const auto& [index, dir] : _indices_dirs) {
            indices_dirs << "index id is: " << index.first << " , index suffix is: " << index.second
                         << " , index dir is: " << dir->toString();
        }
        return fmt::format(
                "inverted index file writer debug string: index storage format is: {}, index path "
                "prefix is: {}, rowset id is: {}, seg id is: {}, closed is: {}, total file size "
                "is: {}, index dirs is: {}",
                _storage_format, _index_path_prefix, _rowset_id, _seg_id, _closed, _total_file_size,
                indices_dirs.str());
    }

private:
    // Helper functions shared between write_v1 and write_v2
    std::vector<FileInfo> prepare_sorted_files(lucene::store::Directory* directory);
    void sort_files(std::vector<FileInfo>& file_infos);
    void copyFile(const char* fileName, lucene::store::Directory* dir,
                  lucene::store::IndexOutput* output, uint8_t* buffer, int64_t bufferLength);
    void finalize_output_dir(lucene::store::Directory* out_dir);
    void add_index_info(int64_t index_id, const std::string& index_suffix,
                        int64_t compound_file_size);
    int64_t headerLength();
    // Helper functions specific to write_v1
    std::pair<int64_t, int32_t> calculate_header_length(const std::vector<FileInfo>& sorted_files,
                                                        lucene::store::Directory* directory);
    virtual std::pair<lucene::store::Directory*, std::unique_ptr<lucene::store::IndexOutput>>
    create_output_stream_v1(int64_t index_id, const std::string& index_suffix);
    virtual void write_header_and_data_v1(lucene::store::IndexOutput* output,
                                          const std::vector<FileInfo>& sorted_files,
                                          lucene::store::Directory* directory,
                                          int64_t header_length, int32_t header_file_count);
    // Helper functions specific to write_v2
    virtual std::pair<lucene::store::Directory*, std::unique_ptr<lucene::store::IndexOutput>>
    create_output_stream_v2();
    void write_version_and_indices_count(lucene::store::IndexOutput* output);
    struct FileMetadata {
        int64_t index_id;
        std::string index_suffix;
        std::string filename;
        int64_t offset;
        int64_t length;
        lucene::store::Directory* directory;

        FileMetadata(int64_t id, const std::string& suffix, const std::string& file, int64_t off,
                     int64_t len, lucene::store::Directory* dir)
                : index_id(id),
                  index_suffix(suffix),
                  filename(file),
                  offset(off),
                  length(len),
                  directory(dir) {}
    };
    std::vector<FileMetadata> prepare_file_metadata_v2(int64_t& current_offset);
    virtual void write_index_headers_and_metadata(lucene::store::IndexOutput* output,
                                                  const std::vector<FileMetadata>& file_metadata);
    void copy_files_data_v2(lucene::store::IndexOutput* output,
                            const std::vector<FileMetadata>& file_metadata);
    Status _insert_directory_into_map(int64_t index_id, const std::string& index_suffix,
                                      std::shared_ptr<DorisFSDirectory> dir);
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
};
} // namespace segment_v2
} // namespace doris
