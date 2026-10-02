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

#include "storage/index/query/docid_set_ops.h"

#include <limits>
#include <roaring/roaring.hh>
#include <utility>

namespace doris::index_query {

Status fabricate_null_disjoint_count_bitmap(uint64_t count, const roaring::Roaring& nulls,
                                            roaring::Roaring* out) {
    roaring::Roaring result;
    if (count > 0) {
        // [0, count + |nulls|) holds at least `count` non-null ids, since at most |nulls| of its
        // members are null.
        const uint64_t window_end = count + nulls.cardinality();
        if (window_end > uint64_t(std::numeric_limits<uint32_t>::max()) + 1) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "fabricate_null_disjoint_count_bitmap: count {} + null count {} exceeds the "
                    "uint32 docid domain (corrupt df or null bitmap)",
                    count, nulls.cardinality());
        }
        result.addRange(0, window_end);
        result -= nulls;
        uint32_t last_kept = 0;
        // Keep exactly the first `count` survivors (select ranks are 0-based).
        if (!result.select(static_cast<uint32_t>(count - 1), &last_kept)) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "fabricate_null_disjoint_count_bitmap: window [0, {}) holds fewer than {} "
                    "non-null ids (corrupt df or null bitmap)",
                    window_end, count);
        }
        result.removeRange(uint64_t(last_kept) + 1, window_end);
    }
    *out = std::move(result);
    return Status::OK();
}

} // namespace doris::index_query
