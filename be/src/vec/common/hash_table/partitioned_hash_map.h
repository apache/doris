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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/HashTable/TwoLevelHashMap.h
// and modified by Doris
#pragma once

#include "vec/common/hash_table/hash_map.h"
#include "vec/common/hash_table/partitioned_hash_table.h"
#include "vec/common/hash_table/ph_hash_map.h"

template <typename ImplTable>
class PartitionedHashMapTable : public PartitionedHashTable<ImplTable> {
public:
    using Impl = ImplTable;
    using Base = PartitionedHashTable<ImplTable>;
    using Key = typename ImplTable::key_type;
    using LookupResult = typename Impl::LookupResult;

    using Base::Base;
    using Base::prefetch;

    using mapped_type = typename Base::mapped_type;

    auto& ALWAYS_INLINE operator[](const Key& x) {
        LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted);

        if (inserted) {
            new (lookup_result_get_mapped(it)) mapped_type();
        }

        return *lookup_result_get_mapped(it);
    }

    template <typename Func>
    void for_each_mapped(Func&& func) {
        for (auto& v : *this) {
            func(v.get_second());
        }
    }
};

template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>>
using PartitionedHashMap =
        PartitionedHashMapTable<HashMap<Key, Mapped, Hash, PartitionedHashTableGrower<>>>;

template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>>
using PHPartitionedHashMap = PartitionedHashMapTable<PHHashMap<Key, Mapped, Hash, true>>;

template <typename Key, typename Mapped, typename Hash>
struct HashTableTraits<PartitionedHashMap<Key, Mapped, Hash>> {
    static constexpr bool is_phmap = false;
    static constexpr bool is_string_hash_table = false;
    static constexpr bool is_partitioned_table = true;
};

template <template <typename> class Derived, typename Key, typename Mapped, typename Hash>
struct HashTableTraits<Derived<PartitionedHashMap<Key, Mapped, Hash>>> {
    static constexpr bool is_phmap = false;
    static constexpr bool is_string_hash_table = false;
    static constexpr bool is_partitioned_table = true;
};

template <typename Key, typename Mapped, typename Hash>
struct HashTableTraits<PHPartitionedHashMap<Key, Mapped, Hash>> {
    static constexpr bool is_phmap = true;
    static constexpr bool is_string_hash_table = false;
    static constexpr bool is_partitioned_table = true;
};

template <template <typename> class Derived, typename Key, typename Mapped, typename Hash>
struct HashTableTraits<Derived<PHPartitionedHashMap<Key, Mapped, Hash>>> {
    static constexpr bool is_phmap = true;
    static constexpr bool is_string_hash_table = false;
    static constexpr bool is_partitioned_table = true;
};
