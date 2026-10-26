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
#include "util/hash_util.hpp"

namespace doris::segment_v2 {

// The sums of norm lengths that merge-on-write BM25 statistics need of an index, kept with the
// index's reader in the searcher cache, so they live and are evicted with it. The index never
// changes, so the sums are computed once and every later query reads no norms. They take a few
// dozen bytes per field.
class NormLengthSums {
public:
    // The sum of the lengths the norms of the non-NULL documents of `field` stand for, computed
    // by `compute` the first time.
    Status non_null_norm_length_sum(const std::wstring& field,
                                    const std::function<Status(uint64_t*)>& compute,
                                    uint64_t* out) {
        {
            std::lock_guard lock(_mutex);
            if (auto it = _non_null_sums.find(field); it != _non_null_sums.end()) {
                *out = it->second;
                return Status::OK();
            }
        }
        // Computed outside the lock, which other queries of the index take to read their sums.
        // Two queries may both compute a sum the index does not hold yet; both get the same.
        RETURN_IF_ERROR(compute(out));
        std::lock_guard lock(_mutex);
        _non_null_sums.emplace(field, *out);
        return Status::OK();
    }

    // The sum of the lengths the norms of the non-NULL documents among `docs`, ascending rows of
    // the index, stand for, computed by `compute` the first time the field meets that set of rows.
    // Queries at one read version hide the same rows, so they read no norms; a few sets per field
    // are kept for queries at different versions, the oldest given up first.
    Status hidden_norm_length_sum(const std::wstring& field, std::span<const uint32_t> docs,
                                  const std::function<Status(uint64_t*)>& compute, uint64_t* out) {
        const RowSetKey key {
                .fingerprint = HashUtil::xxHash64WithSeed(
                        reinterpret_cast<const char*>(docs.data()), docs.size_bytes(), 0),
                .count = docs.size(),
                .first = docs.empty() ? 0 : docs.front(),
                .last = docs.empty() ? 0 : docs.back()};
        auto find = [this, &field, &key](uint64_t* sum) {
            for (const auto& [cached_key, cached_sum] : _hidden_sums[field]) {
                if (cached_key == key) {
                    *sum = cached_sum;
                    return true;
                }
            }
            return false;
        };
        {
            std::lock_guard lock(_mutex);
            if (find(out)) {
                return Status::OK();
            }
        }
        RETURN_IF_ERROR(compute(out));
        std::lock_guard lock(_mutex);
        uint64_t kept = 0;
        if (find(&kept)) {
            // Another query computed the same set meanwhile.
            return Status::OK();
        }
        auto& sums = _hidden_sums[field];
        if (sums.size() == kHiddenSetsPerField) {
            sums.erase(sums.begin());
        }
        sums.emplace_back(key, *out);
        return Status::OK();
    }

private:
    struct RowSetKey {
        uint64_t fingerprint;
        size_t count;
        uint32_t first;
        uint32_t last;
        bool operator==(const RowSetKey&) const = default;
    };
    static constexpr size_t kHiddenSetsPerField = 4;

    std::mutex _mutex;
    std::unordered_map<std::wstring, uint64_t> _non_null_sums;
    std::unordered_map<std::wstring, std::vector<std::pair<RowSetKey, uint64_t>>> _hidden_sums;
};

} // namespace doris::segment_v2
