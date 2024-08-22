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

#include <stdint.h>
#include <string>

#include "io/fs/file_system.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_writer.h"
#include "common/status.h"
#include "mindann/mindann_index_builder.h"
#include "olap/tablet_schema.h"
#include "sparse_vector/sparse_inverted_index_builder.h"
#include "vec/columns/column.h"
#include "mindann/mindann_index_utils.h"
#include "olap/rowset/segment_v2/vector_index_desc.h"

namespace doris {
class CollectionValue;

class Field;

class TabletIndex;

namespace segment_v2 {

class VectorIndexWriter {
public:
    VectorIndexWriter(io::FileSystemSPtr& fs,
                      std::string segment_index_path,
                      bool src_is_nullable) :
                      _fs(fs),
                      _segment_index_path(std::move(segment_index_path)),
                      _src_is_nullable(src_is_nullable) {}

    Status init(const TabletIndex* tablet_index);

    VectorIndexWriter() = default;
    ~VectorIndexWriter() = default;

    // for dense vector index
    Status add_array_float_values(const uint8_t* raw_data_ptr,
                                  const size_t count,
                                  const uint8_t* offsets_ptr);

    // for sparse vector index
    Status add_map_int_to_float_values(const uint8_t* raw_key_data_ptr,
                                       const uint8_t* raw_value_data_ptr,
                                       const size_t count,
                                       const uint8_t* offsets_ptr);

    Status finish();

    int64_t size();

    // we should make sure the independence of MindAnn index, include data and metadata, to make [IndexScanNode] simple
    // enough in the future other than to read the meta both in DataMind and MindAnn.
    // Furthermore, MindAnn index within tablet level should decouple with segment, therefore we should do the empty mark
    // by marking the index file.
    Status flush_empty(const std::string& segment_index_path);

private:
    io::FileWriterPtr _file_writer;
    io::FileSystemSPtr _fs;
    std::string _segment_index_path;
    bool _src_is_nullable;

    // 0 for faiss; 1 for milvus
    u_int8_t _index_family = 0;

    // for faiss dense
    std::unique_ptr<MindAnnIndexBuilder> _dense_index_builder;

    // for milvus sparse
    std::unique_ptr<sparse::SparseInvertedIndexBuilder> _sparse_index_builder;

};

} // namespace segment_v2
} // namespace doris
