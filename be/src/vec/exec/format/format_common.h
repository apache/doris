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

#include "vec/common/int_exp.h"
#include "vec/core/types.h"

namespace doris::vectorized {

struct DecimalScaleParams {
    enum ScaleType {
        NOT_INIT,
        NO_SCALE,
        SCALE_UP,
        SCALE_DOWN,
    };
    ScaleType scale_type = ScaleType::NOT_INIT;
    int64_t scale_factor = 1;

    template <typename DecimalPrimitiveType>
    static inline constexpr DecimalPrimitiveType::NativeType get_scale_factor(int32_t n) {
        if constexpr (std::is_same_v<DecimalPrimitiveType, Decimal32>) {
            return common::exp10_i32(n);
        } else if constexpr (std::is_same_v<DecimalPrimitiveType, Decimal64>) {
            return common::exp10_i64(n);
        } else if constexpr (std::is_same_v<DecimalPrimitiveType, Decimal128V2>) {
            return common::exp10_i128(n);
        } else if constexpr (std::is_same_v<DecimalPrimitiveType, Decimal128V3>) {
            return common::exp10_i128(n);
        } else if constexpr (std::is_same_v<DecimalPrimitiveType, Decimal256>) {
            return common::exp10_i256(n);
        } else {
            static_assert(!sizeof(DecimalPrimitiveType),
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

    ~KVCache() {
        for (auto& kv : _storage) {
            _delete_fn[kv.first](kv.second);
        }
    }

    void erase(const KType& key) {
        std::lock_guard<std::mutex> lock(_lock);
        auto it = _storage.find(key);
        if (it != _storage.end()) {
            _delete_fn[key](_storage[key]);
            _storage.erase(key);
            _delete_fn.erase(key);
        }
    }

    template <class T>
    T* get(const KType& key, const std::function<T*()> create_func) {
        std::lock_guard<std::mutex> lock(_lock);
        auto it = _storage.find(key);
        if (it != _storage.end()) {
            return reinterpret_cast<T*>(it->second);
        } else {
            T* rawPtr = create_func();
            if (rawPtr != nullptr) {
                _delete_fn[key] = [](void* obj) { delete reinterpret_cast<T*>(obj); };
                _storage[key] = rawPtr;
            }
            return rawPtr;
        }
    }

private:
    using DeleteFn = void (*)(void*);

    std::mutex _lock;
    std::unordered_map<KType, DeleteFn> _delete_fn;
    std::unordered_map<KType, void*> _storage;
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
    T* get(const std::string& key, const std::function<T*()> create_func) {
        return _shards[_get_idx(key)]->get(key, create_func);
    }

private:
    uint32_t _get_idx(const std::string& key) {
        return (uint32_t)std::hash<std::string>()(key) % _num_shards;
    }

    uint32_t _num_shards;
    KVCache<std::string>** _shards = nullptr;
};

} // namespace doris::vectorized
