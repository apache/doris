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

#include "olap/rowset/segment_v2/ann_index/ann_index_result_cache/ann_index_topn_cache_handle.h"

#include <cstring>

#include "olap/rowset/segment_v2/ann_index/ann_search_params.h"

namespace doris::segment_v2 {
IndexSearchResult AnnIndexTopnCacheHandle::to_index_search_result() const {
    IndexSearchResult result;
    size_t rows = row_ids ? row_ids->size() : 0;
    if (distances != nullptr) {
        result.distances = std::make_unique<float[]>(rows);
        std::memcpy(result.distances.get(), distances.get(), rows * sizeof(float));
    }
    if (row_ids != nullptr) {
        result.row_ids = std::make_unique<std::vector<uint64_t>>(*row_ids);
    }

    result.roaring = roaring;
    return result;
}
} // namespace doris::segment_v2
