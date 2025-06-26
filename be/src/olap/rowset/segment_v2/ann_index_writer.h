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
#include <CLucene/analysis/LanguageBasedAnalyzer.h>
#include <CLucene/util/bkd/bkd_writer.h>
#include <glog/logging.h>

#include <cstdint>
#include <memory>
#include <roaring/roaring.hh>
#include <string>

#include "common/config.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/index_file_writer.h"
#include "olap/rowset/segment_v2/index_writer.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "olap/tablet_schema.h"
#include "runtime/collection_value.h"
#include "vector/vector_index.h"

namespace doris::segment_v2 {

const int32_t MAX_FIELD_LEN = 0x7FFFFFFFL;
const int32_t MERGE_FACTOR = 100000000;
const int32_t MAX_LEAF_COUNT = 1024;
const float MAXMBSortInHeap = 512.0 * 8;
const int DIMS = 1;

class AnnIndexColumnWriter : public IndexColumnWriter {
public:
    static constexpr const char* INDEX_TYPE = "index_type";
    static constexpr const char* METRIC_TYPE = "metric_type";
    static constexpr const char* QUANTILIZER = "quantilizer";
    static constexpr const char* PQ_M = "pq_m";
    static constexpr const char* DIM = "dim";
    static constexpr const char* MAX_DEGREE = "max_degree";

    explicit AnnIndexColumnWriter(const std::string& field_name, IndexFileWriter* index_file_writer,
                                  const TabletIndex* index_meta, const bool single_field = true);

    ~AnnIndexColumnWriter() override;

    Status init() override;
    void close_on_error() override;
    Status add_nulls(uint32_t count) override;
    Status add_array_nulls(const uint8_t* null_map, size_t num_rows) override;
    Status add_values(const std::string fn, const void* values, size_t count) override;
    Status add_array_values(size_t field_size, const void* value_ptr, const uint8_t* null_map,
                            const uint8_t* offsets_ptr, size_t count) override;
    Status add_array_values(size_t field_size, const CollectionValue* values,
                            size_t count) override;
    int64_t size() const override;
    Status finish() override;

private:
    // VectorIndex shoule be managed by some cache.
    // VectorIndex should be weak shared by AnnIndexWriter and VectorIndexReader
    // This should be a weak_ptr
    std::shared_ptr<VectorIndex> _vector_index;
    IndexFileWriter* _index_file_writer;
    std::wstring _field_name;
    const TabletIndex* _index_meta;
    std::shared_ptr<DorisFSDirectory> _dir;
};

} // namespace doris::segment_v2
