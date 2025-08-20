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

#include "index_storage_format.h"

namespace doris::segment_v2 {

struct FileMetadata {
    int64_t index_id;
    std::string index_suffix;
    std::string filename;
    int64_t offset;
    int64_t length;
    lucene::store::Directory* directory;

    FileMetadata(int64_t id, std::string suffix, std::string file, int64_t off, int64_t len,
                 lucene::store::Directory* dir);
};

class IndexStorageFormatV2 : public IndexStorageFormat {
public:
    IndexStorageFormatV2(IndexFileWriter* index_file_writer);
    ~IndexStorageFormatV2() override = default;

    Status write() override;

private:
    struct MetaFileRange {
        int64_t start_offset;
        int64_t end_offset;
    };

    int64_t header_length();
    void prepare_file_metadata(int64_t& current_offset, std::vector<FileMetadata>& file_metadata,
                               MetaFileRange& meta_range);
    virtual std::pair<std::unique_ptr<lucene::store::Directory, DirectoryDeleter>,
                      std::unique_ptr<lucene::store::IndexOutput>>
    create_output_stream();
    void write_version_and_indices_count(lucene::store::IndexOutput* output);
    virtual void write_index_headers_and_metadata(lucene::store::IndexOutput* output,
                                                  const std::vector<FileMetadata>& file_metadata);
    void copy_files_data(lucene::store::IndexOutput* output,
                         const std::vector<FileMetadata>& file_metadata);
    void add_meta_files_to_index_cache(const MetaFileRange& meta_range);

    friend class IndexFileWriterTest;
};

} // namespace doris::segment_v2