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

class IndexStorageFormatV1 : public IndexStorageFormat {
public:
    IndexStorageFormatV1(IndexFileWriter* index_file_writer);
    ~IndexStorageFormatV1() override = default;

    Status write() override;

private:
    std::pair<int64_t, int32_t> calculate_header_length(const std::vector<FileInfo>& sorted_files,
                                                        lucene::store::Directory* directory);
    virtual std::pair<std::unique_ptr<lucene::store::Directory, DirectoryDeleter>,
                      std::unique_ptr<lucene::store::IndexOutput>>
    create_output_stream(int64_t index_id, const std::string& index_suffix);
    virtual void write_header_and_data(lucene::store::IndexOutput* output,
                                       const std::vector<FileInfo>& sorted_files,
                                       lucene::store::Directory* directory, int64_t header_length,
                                       int32_t header_file_count);
    void add_index_info(int64_t index_id, const std::string& index_suffix,
                        int64_t compound_file_size);

    friend class IndexFileWriterTest;
};

} // namespace doris::segment_v2