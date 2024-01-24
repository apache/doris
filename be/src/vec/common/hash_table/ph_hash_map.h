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

#include <boost/noncopyable.hpp>
#include <span>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/common/hash_table/hash.h"
#include "vec/common/hash_table/phmap_fwd_decl.h"

template <typename Key, typename Mapped>
ALWAYS_INLINE inline auto lookup_result_get_mapped(std::pair<const Key, Mapped>* it) {
    return &(it->second);
}

template <typename Key, typename Mapped, typename HashMethod = DefaultHash<Key>,
          bool PartitionedHashTable = false>
class PHHashMap : private boost::noncopyable {
public:
    using Self = PHHashMap;
    using Hash = HashMethod;
    using cell_type = std::pair<const Key, Mapped>;
    using HashMapImpl = doris::vectorized::flat_hash_map<Key, Mapped, Hash>;

    using key_type = Key;
    using mapped_type = Mapped;
    using value_type = std::pair<const Key, Mapped>;

    using LookupResult = std::pair<const Key, Mapped>*;
    using ConstLookupResult = const std::pair<const Key, Mapped>*;

    using const_iterator_impl = typename HashMapImpl::const_iterator;
    using iterator_impl = typename HashMapImpl::iterator;

    PHHashMap() = default;

    PHHashMap(size_t reserve_for_num_elements) { _hash_map.reserve(reserve_for_num_elements); }

    PHHashMap(PHHashMap&& other) { *this = std::move(other); }

    PHHashMap& operator=(PHHashMap&& rhs) {
        _hash_map.clear();
        _hash_map = std::move(rhs._hash_map);
        std::swap(_need_partition, rhs._need_partition);
        std::swap(_partitioned_threshold, rhs._partitioned_threshold);

        return *this;
    }

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

        auto get_ptr() const { return this; }
        size_t get_hash() const { return base_iterator->get_hash(); }
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
        inserted = false;
        it = &*_hash_map.lazy_emplace(key_holder, [&](const auto& ctor) {
            inserted = true;
            ctor(key_holder, nullptr);
        });

        if constexpr (PartitionedHashTable) {
            _check_if_need_partition();
        }
    }

    template <typename KeyHolder, typename Func>
    void ALWAYS_INLINE lazy_emplace(KeyHolder&& key_holder, LookupResult& it, Func&& f) {
        it = &*_hash_map.lazy_emplace(key_holder, [&](const auto& ctor) { f(ctor, key_holder); });

        if constexpr (PartitionedHashTable) {
            _check_if_need_partition();
        }
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder&& key, LookupResult& it, bool& inserted,
                               size_t hash_value) {
        inserted = false;
        it = &*_hash_map.lazy_emplace_with_hash(key, hash_value, [&](const auto& ctor) {
            inserted = true;
            if constexpr (std::is_pointer_v<std::remove_reference_t<mapped_type>>) {
                ctor(key, nullptr);
            } else {
                ctor(key, mapped_type());
            }
        });

        if constexpr (PartitionedHashTable) {
            _check_if_need_partition();
        }
    }

    template <typename KeyHolder, typename Func>
    void ALWAYS_INLINE lazy_emplace(KeyHolder&& key, LookupResult& it, size_t hash_value,
                                    Func&& f) {
        it = &*_hash_map.lazy_emplace_with_hash(key, hash_value,
                                                [&](const auto& ctor) { f(ctor, key, key); });

        if constexpr (PartitionedHashTable) {
            _check_if_need_partition();
        }
    }

    void ALWAYS_INLINE insert(const Key& key, size_t hash_value, const Mapped& value) {
        auto it = &*_hash_map.lazy_emplace_with_hash(key, hash_value,
                                                     [&](const auto& ctor) { ctor(key, value); });
        it->second = value;
    }

    template <typename KeyHolder>
    LookupResult ALWAYS_INLINE find(KeyHolder&& key) {
        auto it = _hash_map.find(key);
        return it != _hash_map.end() ? &*it : nullptr;
    }

    template <typename KeyHolder>
    LookupResult ALWAYS_INLINE find(KeyHolder&& key, size_t hash_value) {
        auto it = _hash_map.find(key, hash_value);
        return it != _hash_map.end() ? &*it : nullptr;
    }

    size_t hash(const Key& x) const { return _hash_map.hash(x); }

    template <bool read>
    void ALWAYS_INLINE prefetch(const Key& key, size_t hash_value) {
        _hash_map.prefetch_hash(hash_value);
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

    size_t get_buffer_size_in_cells() const { return _hash_map.capacity(); }

    bool add_elem_size_overflow(size_t row) const {
        const auto capacity = _hash_map.capacity();
        // phmap use 7/8th as maximum load factor.
        return (_hash_map.size() + row) > (capacity * 7 / 8);
    }

    size_t size() const { return _hash_map.size(); }
    template <typename MappedType>
    char* get_null_key_data() {
        return nullptr;
    }
    bool has_null_key_data() const { return false; }

    bool need_partition() { return _need_partition; }

    void set_partitioned_threshold(int threshold) { _partitioned_threshold = threshold; }

    bool check_if_need_partition(size_t bucket_count) {
        if constexpr (PartitionedHashTable) {
            return _partitioned_threshold > 0 && bucket_count >= _partitioned_threshold;
        } else {
            return false;
        }
    }

    bool empty() const { return _hash_map.empty(); }

    void clear_and_shrink() { _hash_map.clear(); }

    void expanse_for_add_elem(size_t num_elem) { _hash_map.reserve(num_elem); }

private:
    void _check_if_need_partition() {
        if (UNLIKELY(check_if_need_partition(_hash_map.size() + 1))) {
            _need_partition = add_elem_size_overflow(1);
        }
    }

    HashMapImpl _hash_map;
    // the bucket count threshold above which it's converted to partioned hash table
    // > 0: enable convert dynamically
    // 0: convert is disabled
    int _partitioned_threshold = 0;
    // if need resize and bucket count after resize will be >= _partitioned_threshold,
    // this flag is set to true, and resize does not actually happen,
    // PartitionedHashTable will convert this hash table to partitioned hash table
    bool _need_partition;
};
