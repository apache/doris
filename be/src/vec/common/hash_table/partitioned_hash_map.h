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

template <typename Key, typename Cell, typename Hash = DefaultHash<Key>,
          typename Grower = PartitionedHashTableGrower<>, typename Allocator = HashTableAllocator,
          template <typename...> typename ImplTable = HashMapTable>
class PartitionedHashMapTable
        : public PartitionedHashTable<Key, Cell, Hash, Grower, Allocator,
                                      ImplTable<Key, Cell, Hash, Grower, Allocator>> {
public:
    using Impl = ImplTable<Key, Cell, Hash, Grower, Allocator>;
    using Base = PartitionedHashTable<Key, Cell, Hash, Grower, Allocator,
                                      ImplTable<Key, Cell, Hash, Grower, Allocator>>;
    using LookupResult = typename Impl::LookupResult;

    using Base::Base;
    using Base::prefetch;

    using mapped_type = typename Cell::Mapped;

    typename Cell::Mapped& ALWAYS_INLINE operator[](const Key& x) {
        LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted);

        if (inserted) new (lookup_result_get_mapped(it)) mapped_type();

        return *lookup_result_get_mapped(it);
    }
};

template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>,
          typename Grower = PartitionedHashTableGrower<>, typename Allocator = HashTableAllocator,
          template <typename...> typename ImplTable = HashMapTable>
using PartitionedHashMap = PartitionedHashMapTable<Key, HashMapCell<Key, Mapped, Hash>, Hash,
                                                   Grower, Allocator, ImplTable>;

template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>,
          typename Grower = PartitionedHashTableGrower<>, typename Allocator = HashTableAllocator,
          template <typename...> typename ImplTable = HashMapTable>
using PartitionedHashMapWithSavedHash =
        PartitionedHashMapTable<Key, HashMapCellWithSavedHash<Key, Mapped, Hash>, Hash, Grower,
                                Allocator, ImplTable>;
