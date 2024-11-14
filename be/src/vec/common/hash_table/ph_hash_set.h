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

#include <boost/core/noncopyable.hpp>

#include "vec/common/hash_table/hash.h"
#include "vec/common/hash_table/phmap_fwd_decl.h"

ALWAYS_INLINE inline void* lookup_result_get_mapped(void*) {
    return nullptr;
}

template <typename Key, typename HashMethod = DefaultHash<Key>>
class PHHashSet : private boost::noncopyable {
public:
    using Self = PHHashSet;
    using Hash = HashMethod;
    using HashSetImpl = doris::vectorized::flat_hash_set<Key, Hash>;

    using key_type = Key;
    using mapped_type = void;
    using value_type = void;
    using Value = void*;

    using LookupResult = void*;

    PHHashSet() = default;

    PHHashSet(size_t reserve_for_num_elements) { _hash_set.reserve(reserve_for_num_elements); }

    PHHashSet(PHHashSet&& other) { *this = std::move(other); }

    PHHashSet& operator=(PHHashSet&& rhs) {
        _hash_set.clear();
        _hash_set = std::move(rhs._hash_set);
        return *this;
    }

    size_t hash(const Key& x) const { return _hash_set.hash(x); }

    template <typename KeyHolder, typename Func>
    void ALWAYS_INLINE lazy_emplace(KeyHolder&& key_holder, LookupResult& it, Func&& f) {
        _hash_set.lazy_emplace(key_holder, [&](const auto& ctor) { f(ctor, key_holder); });
    }

    template <typename KeyHolder, typename Func>
    void ALWAYS_INLINE lazy_emplace(KeyHolder&& key, LookupResult& it, size_t hash_value,
                                    Func&& f) {
        _hash_set.lazy_emplace_with_hash(key, hash_value,
                                         [&](const auto& ctor) { f(ctor, key, key); });
    }

    template <bool read>
    void ALWAYS_INLINE prefetch(const Key& key, size_t hash_value) {
        _hash_set.prefetch_hash(hash_value);
    }

    /// Call func(Mapped &) for each hash map element.
    template <typename Func>
    void for_each_mapped(Func&& func) {
        for (auto& v : *this) {
            func(v.get_second());
        }
    }

    size_t get_buffer_size_in_bytes() const {
        const auto capacity = _hash_set.capacity();
        return capacity * sizeof(typename HashSetImpl::slot_type);
    }

    size_t get_buffer_size_in_cells() const { return _hash_set.capacity(); }

    bool add_elem_size_overflow(size_t row) const {
        const auto capacity = _hash_set.capacity();
        // phmap use 7/8th as maximum load factor.
        return (_hash_set.size() + row) > (capacity * 7 / 8);
    }

    size_t size() const { return _hash_set.size(); }

    template <typename MappedType>
    void* get_null_key_data() {
        return nullptr;
    }

    bool has_null_key_data() const { return false; }

    bool empty() const { return _hash_set.empty(); }

    void clear_and_shrink() { _hash_set.clear(); }

    void reserve(size_t num_elem) { _hash_set.reserve(num_elem); }

private:
    HashSetImpl _hash_set;
};
