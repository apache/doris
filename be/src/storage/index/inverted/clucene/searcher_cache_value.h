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

#include <cstdint>
#include <functional>
#include <mutex>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/clucene/inverted_index_searcher.h"
#include "storage/index/inverted/common/inverted_index_cache.h"
#include "util/hash_util.hpp"

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

    // The sum of the lengths the norms of `docs`, ascending rows of the index, stand for, computed
    // by `compute` the first time the field meets that set of rows. Queries at one read version
    // hide the same rows, so they read no norms; a few sets per field are kept for queries at
    // different versions.
    Status hidden_norm_length_sum(const std::wstring& field, std::span<const uint32_t> docs,
                                  const std::function<Status(uint64_t*)>& compute, uint64_t* out) {
        const RowSetKey key {
                .fingerprint = HashUtil::xxHash64WithSeed(
                        reinterpret_cast<const char*>(docs.data()), docs.size_bytes(), 0),
                .count = docs.size(),
                .first = docs.empty() ? 0 : docs.front(),
                .last = docs.empty() ? 0 : docs.back()};
        std::lock_guard lock(_norm_length_sums_mutex);
        auto& sums = _hidden_length_sums[field];
        for (const auto& [cached_key, sum] : sums) {
            if (cached_key == key) {
                *out = sum;
                return Status::OK();
            }
        }
        RETURN_IF_ERROR(compute(out));
        if (sums.size() == kHiddenSetsPerField) {
            sums.erase(sums.begin());
        }
        sums.emplace_back(key, *out);
        return Status::OK();
    }

    IndexSearcherPtr searcher;

private:
    struct RowSetKey {
        uint64_t fingerprint;
        size_t count;
        uint32_t first;
        uint32_t last;
        bool operator==(const RowSetKey&) const = default;
    };
    static constexpr size_t kHiddenSetsPerField = 4;

    std::mutex _norm_length_sums_mutex;
    std::unordered_map<std::wstring, uint64_t> _norm_length_sums;
    std::unordered_map<std::wstring, std::vector<std::pair<RowSetKey, uint64_t>>>
            _hidden_length_sums;
};

} // namespace doris::segment_v2
