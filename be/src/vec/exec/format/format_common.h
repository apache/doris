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
    int32_t scale_factor = 1;

    template <typename DecimalPrimitiveType>
    static inline constexpr DecimalPrimitiveType get_scale_factor(int32_t n) {
        if constexpr (std::is_same_v<DecimalPrimitiveType, Int32>) {
            return common::exp10_i32(n);
        } else if constexpr (std::is_same_v<DecimalPrimitiveType, Int64>) {
            return common::exp10_i64(n);
        } else if constexpr (std::is_same_v<DecimalPrimitiveType, Int128> ||
                             std::is_same_v<DecimalPrimitiveType, Int128I>) {
            return common::exp10_i128(n);
        } else {
            return DecimalPrimitiveType(1);
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
    T* get(const KType& key, const std::function<T*()> createIfNotExists) {
        std::lock_guard<std::mutex> lock(_lock);
        auto it = _storage.find(key);
        if (it != _storage.end()) {
            return reinterpret_cast<T*>(it->second);
        } else {
            T* rawPtr = createIfNotExists();
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

} // namespace doris::vectorized
