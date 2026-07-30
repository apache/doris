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

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "core/data_type/define_primitive_type.h"

namespace doris {

struct QueryDictionaryFilterCacheKey {
    uint64_t expression_digest = 0;
    uint64_t dictionary_hash_low = 0;
    uint64_t dictionary_hash_high = 0;
    uint32_t dictionary_entries = 0;
    PrimitiveType primitive_type = INVALID_TYPE;

    bool operator==(const QueryDictionaryFilterCacheKey&) const = default;
};

struct QueryDictionaryFilterCacheKeyHash {
    size_t operator()(const QueryDictionaryFilterCacheKey& key) const {
        size_t hash = key.expression_digest;
        hash ^= key.dictionary_hash_low + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
        hash ^= key.dictionary_hash_high + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
        hash ^= static_cast<size_t>(key.dictionary_entries) << 1;
        hash ^= static_cast<size_t>(key.primitive_type) << 17;
        return hash;
    }
};

// Query-scoped dictionaries often repeat across row groups and files written by one job. Cache the
// predicate result by dictionary content rather than by file offsets, which are not stable aliases.
class QueryDictionaryFilterCache {
public:
    static constexpr size_t DEFAULT_MAX_BYTES = 16 * 1024 * 1024;

    explicit QueryDictionaryFilterCache(size_t max_bytes = DEFAULT_MAX_BYTES)
            : _max_bytes(max_bytes) {}

    bool lookup(const QueryDictionaryFilterCacheKey& key, std::vector<uint8_t>* result) const {
        std::lock_guard lock(_mutex);
        const auto it = _entries.find(key);
        if (it == _entries.end()) {
            return false;
        }
        *result = it->second;
        return true;
    }

    bool insert(const QueryDictionaryFilterCacheKey& key, std::vector<uint8_t> result) {
        const size_t charge = _entry_charge(result.size());
        if (result.empty() || charge > _max_bytes) {
            return false;
        }
        std::lock_guard lock(_mutex);
        if (_entries.contains(key)) {
            return true;
        }
        if (charge > _max_bytes - _memory_charge) {
            return false;
        }
        _memory_charge += charge;
        _entries.emplace(key, std::move(result));
        return true;
    }

private:
    static constexpr size_t _entry_charge(size_t bitmap_bytes) {
        // Include fixed entry ownership so many tiny dictionaries cannot bypass the query cap.
        return bitmap_bytes + sizeof(QueryDictionaryFilterCacheKey) + sizeof(std::vector<uint8_t>) +
               2 * sizeof(void*);
    }

    const size_t _max_bytes;
    mutable std::mutex _mutex;
    size_t _memory_charge = 0;
    std::unordered_map<QueryDictionaryFilterCacheKey, std::vector<uint8_t>,
                       QueryDictionaryFilterCacheKeyHash>
            _entries;
};

} // namespace doris
