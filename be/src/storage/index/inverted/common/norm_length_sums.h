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

#include <algorithm>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status.h"
#include "util/defer_op.h"
#include "util/hash_util.hpp"

namespace doris::segment_v2 {

// The sums of norm lengths that merge-on-write BM25 statistics need of an index, kept with the
// index's reader in the searcher cache, so they live and are evicted with it. The index never
// changes, so the sums are computed once and every later query reads no norms. They take a few
// dozen bytes per field.
//
// A sum is computed by the first query that needs it. Queries that need the same sum meanwhile
// wait for it rather than read the same norms, while queries of other fields or other hidden rows
// go on: after a load, a burst of queries at the new version would otherwise each read the norms
// of the rows the load hid.
class NormLengthSums {
public:
    // The sum of the lengths the norms of the non-NULL documents of `field` stand for, computed
    // by `compute` the first time.
    Status non_null_norm_length_sum(const std::wstring& field,
                                    const std::function<Status(uint64_t*)>& compute,
                                    uint64_t* out) {
        return _sum(SumKey {.field = field, .rows = std::nullopt}, compute, out);
    }

    // The sum of the lengths the norms of the non-NULL documents among `docs`, ascending rows of
    // the index, stand for, computed by `compute` the first time the field meets that set of rows.
    // Queries at one read version hide the same rows, so they read no norms; a few sets per field
    // are kept for queries at different versions, the oldest given up first.
    Status hidden_norm_length_sum(const std::wstring& field, std::span<const uint32_t> docs,
                                  const std::function<Status(uint64_t*)>& compute, uint64_t* out) {
        const RowSetKey rows {
                .fingerprint = HashUtil::xxHash64WithSeed(
                        reinterpret_cast<const char*>(docs.data()), docs.size_bytes(), 0),
                .count = docs.size(),
                .first = docs.empty() ? 0 : docs.front(),
                .last = docs.empty() ? 0 : docs.back()};
        return _sum(SumKey {.field = field, .rows = rows}, compute, out);
    }

private:
    struct RowSetKey {
        uint64_t fingerprint;
        size_t count;
        uint32_t first;
        uint32_t last;
        bool operator==(const RowSetKey&) const = default;
    };
    // A field's non-NULL sum without `rows`, the sum of a set of its hidden rows with them.
    struct SumKey {
        std::wstring field;
        std::optional<RowSetKey> rows;
        bool operator==(const SumKey&) const = default;
    };
    static constexpr size_t kHiddenSetsPerField = 4;

    Status _sum(const SumKey& key, const std::function<Status(uint64_t*)>& compute, uint64_t* out) {
        {
            std::unique_lock lock(_mutex);
            // A query that finds the sum being computed waits and looks again: the sum is kept
            // then, unless computing it failed, and then this query computes it. Two queries
            // hiding rows {1, 5} of field f at once: the first reads their norms, the second
            // waits and reads the kept sum.
            while (true) {
                if (_find(key, out)) {
                    return Status::OK();
                }
                if (std::ranges::find(_computing, key) == _computing.end()) {
                    break;
                }
                _computed.wait(lock);
            }
            _computing.push_back(key);
        }
        // Computed outside the lock, which other queries of the index take to read their sums.
        // Also when `compute` throws, the key is given up, so a waiting query computes the sum.
        bool kept = false;
        Defer give_up {[&] {
            {
                std::lock_guard lock(_mutex);
                if (kept) {
                    _keep(key, *out);
                }
                _computing.erase(std::ranges::find(_computing, key));
            }
            _computed.notify_all();
        }};
        RETURN_IF_ERROR(compute(out));
        kept = true;
        return Status::OK();
    }

    // Requires _mutex.
    bool _find(const SumKey& key, uint64_t* out) const {
        if (!key.rows.has_value()) {
            const auto it = _non_null_sums.find(key.field);
            if (it == _non_null_sums.end()) {
                return false;
            }
            *out = it->second;
            return true;
        }
        const auto it = _hidden_sums.find(key.field);
        if (it == _hidden_sums.end()) {
            return false;
        }
        for (const auto& [rows, sum] : it->second) {
            if (rows == *key.rows) {
                *out = sum;
                return true;
            }
        }
        return false;
    }

    // Requires _mutex.
    void _keep(const SumKey& key, uint64_t sum) {
        if (!key.rows.has_value()) {
            _non_null_sums.emplace(key.field, sum);
            return;
        }
        auto& sums = _hidden_sums[key.field];
        if (sums.size() == kHiddenSetsPerField) {
            sums.erase(sums.begin());
        }
        sums.emplace_back(*key.rows, sum);
    }

    std::mutex _mutex;
    std::condition_variable _computed;
    // The sums a query is computing, at most one per key.
    std::vector<SumKey> _computing;
    std::unordered_map<std::wstring, uint64_t> _non_null_sums;
    std::unordered_map<std::wstring, std::vector<std::pair<RowSetKey, uint64_t>>> _hidden_sums;
};

} // namespace doris::segment_v2
