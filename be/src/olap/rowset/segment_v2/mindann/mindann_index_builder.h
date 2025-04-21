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
#include "olap/tablet_schema.h"
#include "tenann/builder/index_builder.h"
#include "olap/rowset/segment_v2/common.h"

namespace doris {
namespace segment_v2 {
// A proxy to real Mind ANN index builder
class MindAnnIndexBuilder {
public:
    MindAnnIndexBuilder(const std::string& segment_index_path, bool src_is_nullable)
            : _index_path(segment_index_path), _src_is_nullable(src_is_nullable) {};

    // proxy should not clean index builder resource
    ~MindAnnIndexBuilder() {
        close();
    }

    Status init(const TabletIndex* tabet_index);

    Status add_array_float_values(const uint8_t* raw_data_ptr,
                                  const size_t count,
                                  const uint8_t* offsets_ptr);

    Status flush();

    void close();

    void abort();

    size_t get_added_count() {
        return _values_added;
    }

    int64_t get_vector_index_build_threshold() {
        return _vector_index_build_threshold;
    }

private:
    std::string _index_path;
    bool _src_is_nullable;

    rowid_t _rid = 0;
    std::shared_ptr<tenann::IndexBuilder> _index_builder;
    uint32_t _dim = 0;
    // record the actual amount of data added to the index
    size_t _values_added = 0;
    int64_t _vector_index_build_threshold = 0;

    Status add_array_float_values_not_null(const uint8_t* raw_data_ptr,
                                           const size_t count,
                                           const uint8_t* offsets_ptr);

    Status add_array_float_values_nullable(const uint8_t* raw_data_ptr,
                                           const size_t count,
                                           const uint8_t* offsets_ptr);

};

} // namespace segment_v2
} // namespace doris
