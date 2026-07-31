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

#include "storage/index/ann/ann_search_params.h"

#include "storage/index/ann/ann_index_result_cache/ann_index_result_cache_handle.h"

namespace doris::segment_v2 {

AnnIndexResultCacheHandle IndexSearchResult::to_cache_handle() const {
    AnnIndexResultCacheHandle handle;
    handle.roaring = roaring;
    handle.row_ids = row_ids;
    handle.distances = distances;
    return handle;
}

AnnIndexResultCacheHandle AnnRangeSearchResult::to_cache_handle() const {
    AnnIndexResultCacheHandle handle;
    handle.distances = distance;
    handle.row_ids = row_ids;
    handle.roaring = roaring;
    return handle;
}

AnnRangeSearchResult AnnRangeSearchResult::from_cache_handle(
        const AnnIndexResultCacheHandle& handle) {
    AnnRangeSearchResult result;
    result.roaring = handle.roaring;
    result.row_ids = handle.row_ids;
    result.distance = handle.distances;
    return result;
}

} // namespace doris::segment_v2
