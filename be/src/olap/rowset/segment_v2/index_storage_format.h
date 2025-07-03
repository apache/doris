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

#include "common/status.h"
#include "olap/rowset/segment_v2/inverted_index_common.h"

namespace doris::segment_v2 {

class IndexFileWriter;

class FileInfo {
public:
    std::string filename;
    int64_t filesize;
};

class IndexStorageFormat {
public:
    IndexStorageFormat(IndexFileWriter* index_file_writer);
    virtual ~IndexStorageFormat() = default;

    virtual Status write() = 0;

    void sort_files(std::vector<FileInfo>& file_infos);
    std::vector<FileInfo> prepare_sorted_files(lucene::store::Directory* directory);
    void copy_file(const char* fileName, lucene::store::Directory* dir,
                   lucene::store::IndexOutput* output, uint8_t* buffer, int64_t bufferLength);

protected:
    IndexFileWriter* _index_file_writer = nullptr;
};
using IndexStorageFormatPtr = std::unique_ptr<IndexStorageFormat>;

} // namespace doris::segment_v2