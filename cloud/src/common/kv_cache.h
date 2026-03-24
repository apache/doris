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

#include <gen_cpp/cloud.pb.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

#include "cpp/lru_cache.h"

namespace doris::cloud {

namespace detail {

template <typename T>
inline constexpr bool dependent_false_v = false;

template <typename T>
void encode_key_component(std::string* encoded_key, const T& value) {
    if constexpr (std::is_same_v<std::decay_t<T>, std::string>) {
        uint64_t size = value.size();
        encoded_key->append(reinterpret_cast<const char*>(&size), sizeof(size));
        encoded_key->append(value.data(), value.size());
    } else if constexpr (std::is_same_v<std::decay_t<T>, std::string_view>) {
        uint64_t size = value.size();
        encoded_key->append(reinterpret_cast<const char*>(&size), sizeof(size));
        encoded_key->append(value.data(), value.size());
    } else if constexpr (std::is_enum_v<T>) {
        using UnderlyingType = std::underlying_type_t<T>;
        auto stored = static_cast<UnderlyingType>(value);
        encoded_key->append(reinterpret_cast<const char*>(&stored), sizeof(stored));
    } else if constexpr (std::is_integral_v<T>) {
        encoded_key->append(reinterpret_cast<const char*>(&value), sizeof(value));
    } else {
        static_assert(dependent_false_v<T>, "unsupported key component type");
    }
}

template <typename KeyTuple>
std::string encode_key(const KeyTuple& key) {
    std::string encoded_key;
    std::apply(
            [&encoded_key](const auto&... values) {
                (encode_key_component(&encoded_key, values), ...);
            },
            key);
    return encoded_key;
}

inline int64_t now_seconds() {
    return std::chrono::duration_cast<std::chrono::seconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
}

} // namespace detail

template <typename KeyTuple, typename ValuePB, uint32_t NumShards = 16>
class KvCache {
public:
    explicit KvCache(size_t capacity, int64_t ttl_seconds = 0, std::string name = "cloud_kv_cache")
            : _ttl_seconds(ttl_seconds),
              _cache(std::make_unique<ShardedLRUCache>(
                      std::move(name), capacity, LRUCacheType::NUMBER, NumShards, 0, false)) {}

    bool get(const KeyTuple& key, ValuePB* value) {
        std::string encoded_key = detail::encode_key(key);
        Cache::Handle* handle = _cache->lookup(CacheKey(encoded_key));
        if (handle == nullptr) {
            return false;
        }

        auto* entry = static_cast<CacheEntry*>(_cache->value(handle));
        if (_ttl_seconds > 0 && entry->expire_time < detail::now_seconds()) {
            _cache->release(handle);
            return false;
        }
        *value = entry->value;
        _cache->release(handle);
        return true;
    }

    void put(const KeyTuple& key, const ValuePB& value) {
        std::string encoded_key = detail::encode_key(key);
        auto* entry = new CacheEntry;
        entry->value = value;
        entry->expire_time = _ttl_seconds > 0 ? detail::now_seconds() + _ttl_seconds : 0;
        Cache::Handle* handle =
                _cache->insert(CacheKey(encoded_key), entry, 1, CachePriority::NORMAL,
                               cache_value_deleter<CacheEntry>);
        _cache->release(handle);
    }

    void invalidate(const KeyTuple& key) {
        std::string encoded_key = detail::encode_key(key);
        _cache->erase(CacheKey(encoded_key));
    }

    void clear() { _cache->prune(); }

    size_t size() const { return _cache->get_element_count(); }

private:
    struct CacheEntry {
        ValuePB value;
        int64_t expire_time = 0;
    };

    const int64_t _ttl_seconds;
    std::unique_ptr<ShardedLRUCache> _cache;
};

using TabletIndexCache = KvCache<std::tuple<std::string, int64_t>, TabletIndexPB>;

} // namespace doris::cloud
