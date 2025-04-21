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
#include "sparse_inverted_index.h"

namespace doris::segment_v2::sparse {

class SparseInvertedIndexSearcher {
public:
    SparseInvertedIndexSearcher(io::FileSystemSPtr& fs,
                                std::string segment_index_path,
                                float drop_ratio_search,
                                size_t refine_factor) :
                                _fs(fs),
                                _index_path(std::move(std::move(segment_index_path))),
                                _drop_ratio_search(drop_ratio_search),
                                _refine_factor(refine_factor) {}

    ~SparseInvertedIndexSearcher() {
        close();
    }

    Status init();

    Status search(std::vector<int32_t>* query_vector_id,
                  std::vector<float>* query_vector_val,
                  int k,
                  int64_t* result_ids,
                  uint8_t* result_distances,
                  roaring::Roaring& _row_bitmap);

    Status range_search();

    void close();

private:
    io::FileSystemSPtr _fs;
    std::string _index_path;
    float _drop_ratio_search;
    size_t _refine_factor;

    // index_type=sparse_wand, use_wand=true;
    // index_type=sparse_inverted_index, use_wand=false
    std::shared_ptr<InvertedIndex<true>> _index;
    io::FileReaderSPtr _file_reader;
};

} // namespace doris::segment_v2::sparse

