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

#include <memory>
#include <typeindex>

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
 * It store a object instance global. User can invoke get method by key and a
 * object creator callback. If there is a instance stored in cache, then it will
 * return a void pointer of it, otherwise, it will invoke creator callback, create
 * a new instance store global, and return it.
 *
 * The stored objects will be deleted when deconstructing, so user do not need to
 * delete the returned pointer.
 *
 * User can invoke erase method by key to delete data.
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
        // Destroy the cached value outside the shard lock.
    }

    template <class T>
    T* get(const KType& key, const std::function<T*()> create_func, bool* cache_hit = nullptr) {
        std::shared_ptr<Entry> entry = _get_or_create_entry<T>(key);
        std::lock_guard<std::mutex> entry_lock(entry->_lock);
        if (entry->_value != nullptr) {
            if (cache_hit != nullptr) {
                *cache_hit = true;
            }
            return static_cast<T*>(entry->_value);
        } else {
            if (cache_hit != nullptr) {
                *cache_hit = false;
            }
            T* rawPtr = create_func();
            if (rawPtr != nullptr) {
                entry->_delete_fn = [](void* obj) { delete static_cast<T*>(obj); };
                entry->_value = rawPtr;
            }
            return rawPtr;
        }
    }

private:
    using DeleteFn = void (*)(void*);

    struct Entry {
        ~Entry() {
            if (_value != nullptr) {
                _delete_fn(_value);
            }
        }

        // The shard lock only protects the map. Factory calls hold this entry lock, so an
        // unrelated key can keep loading while another key is doing remote I/O.
        std::mutex _lock;
        void* _value = nullptr;
        DeleteFn _delete_fn = nullptr;
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
    T* get(const std::string& key, const std::function<T*()> create_func,
           bool* cache_hit = nullptr) {
        return _shards[_get_idx(key)]->get(key, create_func, cache_hit);
    }

private:
    uint32_t _get_idx(const std::string& key) const {
        return (uint32_t)std::hash<std::string>()(key) % _num_shards;
    }

    uint32_t _num_shards;
    KVCache<std::string>** _shards = nullptr;
};

} // namespace doris
