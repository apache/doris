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
#include "io/fs/file_system.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/tablet_schema.h"
#include "sparse_inverted_index.h"

namespace doris::segment_v2::sparse {

class SparseInvertedIndexBuilder {
public:
    SparseInvertedIndexBuilder(io::FileSystemSPtr& fs,
                               std::string segment_index_path) :
                               _fs(fs),
                               _index_path(segment_index_path) {}

    ~SparseInvertedIndexBuilder() = default;

    Status init();

    Status add_map_int_to_float_values(const uint8_t* raw_key_data_ptr,
                                       const uint8_t* raw_value_data_ptr,
                                       const size_t count,
                                       const uint8_t* offsets_ptr);

    Status flush();

    Status close();

private:
    io::FileSystemSPtr _fs;
    std::string _index_path;

    std::atomic_bool _closed = false;
    rowid_t _rid = 0;
    std::shared_ptr<InvertedIndex<true>> _index_builder;
    io::FileWriterPtr _file_writer;
};

} // namespace doris::segment_v2::sparse
