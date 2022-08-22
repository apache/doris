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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/HashTable/HashMap.h
// and modified by Doris

#pragma once

#include "vec/common/hash_table/hash_map.h"
#include "vec/common/hash_table/join_hash_table.h"

/** NOTE JoinHashMap could only be used for memmoveable (position independent) types.
  * Example: std::string is not position independent in libstdc++ with C++11 ABI or in libc++.
  * Also, key in hash table must be of type, that zero bytes is compared equals to zero key.
  */

template <typename Key, typename Cell, typename Hash = DefaultHash<Key>,
          typename Grower = HashTableGrower<>, typename Allocator = HashTableAllocator>
class JoinHashMapTable : public JoinHashTable<Key, Cell, Hash, Grower, Allocator> {
public:
    using Self = JoinHashMapTable;
    using Base = JoinHashTable<Key, Cell, Hash, Grower, Allocator>;

    using key_type = Key;
    using value_type = typename Cell::value_type;
    using mapped_type = typename Cell::Mapped;

    using LookupResult = typename Base::LookupResult;

    using JoinHashTable<Key, Cell, Hash, Grower, Allocator>::JoinHashTable;

    /// Merge every cell's value of current map into the destination map via emplace.
    ///  Func should have signature void(Mapped & dst, Mapped & src, bool emplaced).
    ///  Each filled cell in current map will invoke func once. If that map doesn't
    ///  have a key equals to the given cell, a new cell gets emplaced into that map,
    ///  and func is invoked with the third argument emplaced set to true. Otherwise
    ///  emplaced is set to false.
    template <typename Func>
    void ALWAYS_INLINE merge_to_via_emplace(Self& that, Func&& func) {
        for (auto it = this->begin(), end = this->end(); it != end; ++it) {
            typename Self::LookupResult res_it;
            bool inserted;
            that.emplace(it->get_first(), res_it, inserted, it.get_hash());
            func(*lookup_result_get_mapped(res_it), it->get_second(), inserted);
        }
    }

    /// Merge every cell's value of current map into the destination map via find.
    ///  Func should have signature void(Mapped & dst, Mapped & src, bool exist).
    ///  Each filled cell in current map will invoke func once. If that map doesn't
    ///  have a key equals to the given cell, func is invoked with the third argument
    ///  exist set to false. Otherwise exist is set to true.
    template <typename Func>
    void ALWAYS_INLINE merge_to_via_find(Self& that, Func&& func) {
        for (auto it = this->begin(), end = this->end(); it != end; ++it) {
            auto res_it = that.find(it->get_first(), it.get_hash());
            if (!res_it)
                func(it->get_second(), it->get_second(), false);
            else
                func(*lookup_result_get_mapped(res_it), it->get_second(), true);
        }
    }

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

    size_t get_size() {
        size_t count = 0;
        for (auto& v : *this) {
            count += v.get_second().get_row_count();
        }
        return count;
    }

    mapped_type& ALWAYS_INLINE operator[](Key x) {
        typename JoinHashMapTable::LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted);

        /** It may seem that initialization is not necessary for POD-types (or __has_trivial_constructor),
          *  since the hash table memory is initially initialized with zeros.
          * But, in fact, an empty cell may not be initialized with zeros in the following cases:
          * - ZeroValueStorage (it only zeros the key);
          * - after resizing and moving a part of the cells to the new half of the hash table, the old cells also have only the key to zero.
          *
          * On performance, there is almost always no difference, due to the fact that it->second is usually assigned immediately
          *  after calling `operator[]`, and since `operator[]` is inlined, the compiler removes unnecessary initialization.
          *
          * Sometimes due to initialization, the performance even grows. This occurs in code like `++map[key]`.
          * When we do the initialization, for new cells, it's enough to make `store 1` right away.
          * And if we did not initialize, then even though there was zero in the cell,
          *  the compiler can not guess about this, and generates the `load`, `increment`, `store` code.
          */
        if (inserted) new (lookup_result_get_mapped(it)) mapped_type();

        return *lookup_result_get_mapped(it);
    }

    char* get_null_key_data() { return nullptr; }
    bool has_null_key_data() const { return false; }
};

template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>,
          typename Grower = JoinHashTableGrower<>, typename Allocator = HashTableAllocator>
using JoinHashMap = JoinHashMapTable<Key, HashMapCell<Key, Mapped, Hash>, Hash, Grower, Allocator>;

template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>,
          typename Grower = JoinHashTableGrower<>, typename Allocator = HashTableAllocator>
using JoinHashMapWithSavedHash =
        JoinHashMapTable<Key, HashMapCellWithSavedHash<Key, Mapped, Hash>, Hash, Grower, Allocator>;