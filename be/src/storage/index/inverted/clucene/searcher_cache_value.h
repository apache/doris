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

#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include "common/status.h"
#include "storage/index/inverted/clucene/inverted_index_searcher.h"
#include "storage/index/inverted/common/inverted_index_cache.h"

namespace doris::segment_v2 {

struct CluceneSearcherCacheValue final : InvertedIndexSearcherCache::CacheValue {
    CluceneSearcherCacheValue(IndexSearcherPtr opened, size_t mem_size, int64_t visit_time)
            : CacheValue(mem_size, visit_time), searcher(std::move(opened)) {}

    // The sum of the lengths the norms of the non-NULL documents of `field` stand for, computed
    // by `compute` the first time. The index never changes, and merge-on-write scoring needs the
    // sum for every query.
    Status non_null_norm_length_sum(const std::wstring& field,
                                    const std::function<Status(uint64_t*)>& compute,
                                    uint64_t* out) {
        std::lock_guard lock(_norm_length_sums_mutex);
        if (auto it = _norm_length_sums.find(field); it != _norm_length_sums.end()) {
            *out = it->second;
            return Status::OK();
        }
        RETURN_IF_ERROR(compute(out));
        _norm_length_sums.emplace(field, *out);
        return Status::OK();
    }

    IndexSearcherPtr searcher;

private:
    std::mutex _norm_length_sums_mutex;
    std::unordered_map<std::wstring, uint64_t> _norm_length_sums;
};

} // namespace doris::segment_v2
