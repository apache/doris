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
#include <memory>
#include <mutex>
#include <typeindex>
#include <unordered_map>

#include "core/data_type/primitive_type.h"
#include "core/types.h"
#include "exec/common/int_exp.h"

namespace doris {

struct DecimalScaleParams {
    enum ScaleType {
        NOT_INIT,
        NO_SCALE,
        SCALE_UP,
        SCALE_DOWN,
    };
    ScaleType scale_type = ScaleType::NOT_INIT;
    int64_t scale_factor = 1;

    template <PrimitiveType DecimalPrimitiveType>
    static inline constexpr typename PrimitiveTypeTraits<DecimalPrimitiveType>::CppType::NativeType
    get_scale_factor(int32_t n) {
        if constexpr (DecimalPrimitiveType == TYPE_DECIMAL32) {
            return common::exp10_i32(n);
        } else if constexpr (DecimalPrimitiveType == TYPE_DECIMAL64) {
            return common::exp10_i64(n);
        } else if constexpr (DecimalPrimitiveType == TYPE_DECIMALV2) {
            return common::exp10_i128(n);
        } else if constexpr (DecimalPrimitiveType == TYPE_DECIMAL128I) {
            return common::exp10_i128(n);
        } else if constexpr (DecimalPrimitiveType == TYPE_DECIMAL256) {
            return common::exp10_i256(n);
        } else {
            static_assert(!sizeof(typename PrimitiveTypeTraits<DecimalPrimitiveType>::CppType),
                          "All types must be matched with if constexpr.");
        }
    }
};

/**
 * Key-Value Cache Helper.
 *
 * Stores objects by key and value type. get() returns shared ownership of the cached
 * value, invoking the creator on a miss. A null result is not cached and can be retried.
 * Concurrent misses create independently without holding cache locks; the first value
 * published wins. cache_hit is true when returning an already published value, including
 * when another caller publishes while this caller is creating its own value.
 *
 * erase() and cache destruction release only the cache's ownership. For example, a
 * value returned by get() remains alive after erase() until its last caller releases it.
 *
 * @tparam KType is the key type
 */
template <typename KType>
class KVCache {
public:
    KVCache() = default;
    ~KVCache() = default;

    template <class T>
    void erase(const KType& key) {
        std::shared_ptr<Entry> erased_entry;
        {
            std::lock_guard<std::mutex> lock(_lock);
            auto key_it = _entries.find(key);
            if (key_it == _entries.end()) {
                return;
            }
            auto type_it = key_it->second.find(typeid(T));
            if (type_it == key_it->second.end()) {
                return;
            }
            erased_entry = std::move(type_it->second);
            key_it->second.erase(type_it);
            if (key_it->second.empty()) {
                _entries.erase(key_it);
            }
        }
        // Release the cached value outside the shard lock; callers may still own it.
    }

    template <class T>
    std::shared_ptr<T> get(const KType& key, const std::function<std::shared_ptr<T>()>& create_func,
                           bool* cache_hit = nullptr) {
        std::shared_ptr<Entry> entry = _get_or_create_entry<T>(key);
        {
            std::lock_guard<std::mutex> entry_lock(entry->_lock);
            if (entry->_value != nullptr) {
                if (cache_hit != nullptr) {
                    *cache_hit = true;
                }
                return std::static_pointer_cast<T>(entry->_value);
            }
        }
        if (cache_hit != nullptr) {
            *cache_hit = false;
        }
        // A stalled I/O in one factory must not prevent another caller from loading the same key.
        auto value = create_func();
        // Preserve this caller's factory failure even if another caller has since published a value.
        if (value == nullptr) {
            return nullptr;
        }
        // Declared after value so a losing candidate is destroyed after unlocking, even if its
        // destructor calls back into this cache.
        std::lock_guard<std::mutex> entry_lock(entry->_lock);
        // Another caller may have finished its I/O first. Reuse its value instead of replacing it.
        if (entry->_value != nullptr) {
            if (cache_hit != nullptr) {
                *cache_hit = true;
            }
            return std::static_pointer_cast<T>(entry->_value);
        }
        entry->_value = value;
        return value;
    }

private:
    struct Entry {
        // The shard lock protects the map; this lock only protects value lookup and publication.
        // Factories run without either lock, including concurrent loads of the same key.
        std::mutex _lock;
        std::shared_ptr<void> _value;
    };

    template <class T>
    std::shared_ptr<Entry> _get_or_create_entry(const KType& key) {
        std::lock_guard<std::mutex> lock(_lock);
        auto key_it = _entries.try_emplace(key).first;
        auto type_it = key_it->second.find(typeid(T));
        if (type_it != key_it->second.end()) {
            return type_it->second;
        }
        auto entry = std::make_shared<Entry>();
        key_it->second.emplace(typeid(T), entry);
        return entry;
    }

    std::mutex _lock;
    std::unordered_map<KType, std::unordered_map<std::type_index, std::shared_ptr<Entry>>> _entries;
};

class ShardedKVCache {
public:
    ShardedKVCache(uint32_t num_shards) : _num_shards(num_shards) {
        _shards = new (std::nothrow) KVCache<std::string>*[_num_shards];
        for (uint32_t i = 0; i < _num_shards; i++) {
            _shards[i] = new KVCache<std::string>();
        }
    }

    ~ShardedKVCache() {
        for (uint32_t i = 0; i < _num_shards; i++) {
            delete _shards[i];
        }
        delete[] _shards;
    }

    template <class T>
    std::shared_ptr<T> get(const std::string& key,
                           const std::function<std::shared_ptr<T>()>& create_func,
                           bool* cache_hit = nullptr) {
        return _shards[_get_idx(key)]->get<T>(key, create_func, cache_hit);
    }

private:
    uint32_t _get_idx(const std::string& key) const {
        return (uint32_t)std::hash<std::string>()(key) % _num_shards;
    }

    uint32_t _num_shards;
    KVCache<std::string>** _shards = nullptr;
};

} // namespace doris
