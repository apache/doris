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

#include <parallel_hashmap/phmap.h>

#include "vec/common/hash_table/hash.h"
#include "vec/common/hash_table/hash_table_utils.h"

template <typename Key, typename Mapped>
ALWAYS_INLINE inline auto lookup_result_get_mapped(std::pair<const Key, Mapped>* it) {
    return &(it->second);
}

template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>,
          bool use_parallel = false>
class PHHashMap : private boost::noncopyable {
public:
    using Self = PHHashMap;
    using HashMapImpl =
            std::conditional_t<use_parallel, phmap::parallel_flat_hash_map<Key, Mapped, Hash>,
                               phmap::flat_hash_map<Key, Mapped, Hash>>;

    using key_type = Key;
    using mapped_type = Mapped;
    using value_type = std::pair<const Key, Mapped>;

    using LookupResult = std::pair<const Key, Mapped>*;

    using const_iterator_impl = typename HashMapImpl::const_iterator;
    using iterator_impl = typename HashMapImpl::iterator;

    template <typename Derived, bool is_const>
    class iterator_base {
        using BaseIterator = std::conditional_t<is_const, const_iterator_impl, iterator_impl>;

        BaseIterator base_iterator;
        friend class PHHashMap;

    public:
        iterator_base() {}
        iterator_base(BaseIterator it) : base_iterator(it) {}

        bool operator==(const iterator_base& rhs) const {
            return base_iterator == rhs.base_iterator;
        }
        bool operator!=(const iterator_base& rhs) const {
            return base_iterator != rhs.base_iterator;
        }

        Derived& operator++() {
            base_iterator++;
            return static_cast<Derived&>(*this);
        }

        auto& operator*() const { return *this; }
        auto* operator->() const { return this; }

        auto& operator*() { return *this; }
        auto* operator->() { return this; }

        const auto& get_first() const { return base_iterator->first; }

        const auto& get_second() const { return base_iterator->second; }

        auto& get_second() { return base_iterator->second; }

        auto get_ptr() const { return *base_iterator; }
        size_t get_hash() const { return base_iterator->get_hash(); }

        size_t get_collision_chain_length() const { return 0; }
    };

    class iterator : public iterator_base<iterator, false> {
    public:
        using iterator_base<iterator, false>::iterator_base;
    };

    class const_iterator : public iterator_base<const_iterator, true> {
    public:
        using iterator_base<const_iterator, true>::iterator_base;
    };

    const_iterator begin() const { return const_iterator(_hash_map.cbegin()); }

    const_iterator cbegin() const { return const_iterator(_hash_map.cbegin()); }

    iterator begin() { return iterator(_hash_map.begin()); }

    const_iterator end() const { return const_iterator(_hash_map.cend()); }
    const_iterator cend() const { return const_iterator(_hash_map.cend()); }
    iterator end() { return iterator(_hash_map.end()); }

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder&& key_holder, LookupResult& it, bool& inserted) {
        const auto& key = key_holder_get_key(key_holder);
        inserted = false;
        auto it_ = _hash_map.lazy_emplace(key, [&](const auto& ctor) {
            inserted = true;
            key_holder_persist_key(key_holder);
            ctor(key_holder_get_key(key_holder), nullptr);
        });
        it = &*it_;
    }

    template <typename KeyHolder, typename Func>
    void ALWAYS_INLINE lazy_emplace(KeyHolder&& key_holder, LookupResult& it, Func&& f) {
        const auto& key = key_holder_get_key(key_holder);
        auto it_ = _hash_map.lazy_emplace(key, [&](const auto& ctor) {
            key_holder_persist_key(key_holder);
            f(ctor, key);
        });
        it = &*it_;
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder&& key_holder, LookupResult& it, size_t hash_value,
                               bool& inserted) {
        const auto& key = key_holder_get_key(key_holder);
        inserted = false;
        if constexpr (use_parallel) {
            auto it_ = _hash_map.lazy_emplace_with_hash(hash_value, key, [&](const auto& ctor) {
                inserted = true;
                key_holder_persist_key(key_holder);
                ctor(key, nullptr);
            });
            it = &*it_;
        } else {
            auto it_ = _hash_map.lazy_emplace_with_hash(key, hash_value, [&](const auto& ctor) {
                inserted = true;
                key_holder_persist_key(key_holder);
                ctor(key, nullptr);
            });
            it = &*it_;
        }
    }

    template <typename KeyHolder, typename Func>
    void ALWAYS_INLINE lazy_emplace(KeyHolder&& key_holder, LookupResult& it, size_t hash_value,
                                    Func&& f) {
        const auto& key = key_holder_get_key(key_holder);
        if constexpr (use_parallel) {
            auto it_ = _hash_map.lazy_emplace_with_hash(hash_value, key, [&](const auto& ctor) {
                key_holder_persist_key(key_holder);
                f(ctor, key);
            });
            it = &*it_;
        } else {
            auto it_ = _hash_map.lazy_emplace_with_hash(key, hash_value, [&](const auto& ctor) {
                key_holder_persist_key(key_holder);
                f(ctor, key);
            });
            it = &*it_;
        }
    }

    template <typename KeyHolder>
    LookupResult ALWAYS_INLINE find(KeyHolder&& key_holder) {
        const auto& key = key_holder_get_key(key_holder);
        auto it = _hash_map.find(key);
        return it != _hash_map.end() ? &*it : nullptr;
    }

    template <typename KeyHolder>
    LookupResult ALWAYS_INLINE find(KeyHolder&& key_holder, size_t hash_value) {
        const auto& key = key_holder_get_key(key_holder);
        auto it = _hash_map.find(key, hash_value);
        return it != _hash_map.end() ? &*it : nullptr;
    }

    size_t hash(const Key& x) const { return _hash_map.hash(x); }

    void ALWAYS_INLINE prefetch_by_hash(size_t hash_value) {
        if constexpr (!use_parallel) _hash_map.prefetch_hash(hash_value);
    }

    void ALWAYS_INLINE prefetch_by_key(Key key) { _hash_map.prefetch(key); }

    /// Call func(const Key &, Mapped &) for each hash map element.
    template <typename Func>
    void for_each_value(Func&& func) {
        for (auto& v : *this) func(v.get_first(), v.get_second());
    }

    /// Call func(Mapped &) for each hash map element.
    template <typename Func>
    void for_each_mapped(Func&& func) {
        for (auto& v : *this) func(v.get_second());
    }

    size_t get_buffer_size_in_bytes() const {
        const auto capacity = _hash_map.capacity();
        return capacity * sizeof(typename HashMapImpl::slot_type);
    }

    bool add_elem_size_overflow(size_t row) const {
        const auto capacity = _hash_map.capacity();
        // phmap use 7/8th as maximum load factor.
        return (_hash_map.size() + row) > (capacity * 7 / 8);
    }

    size_t size() const { return _hash_map.size(); }

    char* get_null_key_data() { return nullptr; }
    bool has_null_key_data() const { return false; }

    HashMapImpl _hash_map;
};

template <typename Key, typename Mapped, typename Hash, bool use_parallel>
struct HashTableTraits<PHHashMap<Key, Mapped, Hash, use_parallel>> {
    static constexpr bool is_phmap = true;
    static constexpr bool is_parallel_phmap = use_parallel;
    static constexpr bool is_string_hash_table = false;
};
