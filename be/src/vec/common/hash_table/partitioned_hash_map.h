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

template <typename Key, typename Cell, bool ENABLE_PARTITIONED = false,
          typename Hash = DefaultHash<Key>, typename Grower = PartitionedHashTableGrower<>,
          typename Allocator = HashTableAllocator,
          template <typename...> typename ImplTable = HashMapTable>
class PartitionedHashMapTable
        : public PartitionedHashTable<Key, Cell, Hash, Grower, Allocator,
                                      ImplTable<Key, Cell, Hash, Grower, Allocator>,
                                      ENABLE_PARTITIONED> {
public:
    using Impl = ImplTable<Key, Cell, Hash, Grower, Allocator>;
    using Base =
            PartitionedHashTable<Key, Cell, Hash, Grower, Allocator,
                                 ImplTable<Key, Cell, Hash, Grower, Allocator>, ENABLE_PARTITIONED>;
    using LookupResult = typename Impl::LookupResult;

    using Base::Base;
    using Base::prefetch;

    using mapped_type = typename Cell::Mapped;

    template <typename Func>
    void ALWAYS_INLINE for_each_value(Func&& func) {
        for (auto i = 0u; i < this->NUM_LEVEL1_SUB_TABLES; ++i)
            this->level1_sub_tables[i].for_each_value(func);
    }

    typename Cell::Mapped& ALWAYS_INLINE operator[](const Key& x) {
        LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted);

        if (inserted) new (lookup_result_get_mapped(it)) mapped_type();

        return *lookup_result_get_mapped(it);
    }

    size_t get_size() {
        size_t count = 0;
        for (auto i = 0u; i < this->NUM_LEVEL1_SUB_TABLES; ++i) {
            for (auto& v : this->level1_sub_tables[i]) {
                count += v.get_second().get_row_count();
            }
        }
        return count;
    }
};

template <typename Key, typename Mapped, bool ENABLE_PARTITIONED = false,
          typename Hash = DefaultHash<Key>, typename Grower = PartitionedHashTableGrower<>,
          typename Allocator = HashTableAllocator,
          template <typename...> typename ImplTable = HashMapTable>
using PartitionedHashMap =
        PartitionedHashMapTable<Key, HashMapCell<Key, Mapped, Hash>, ENABLE_PARTITIONED, Hash,
                                Grower, Allocator, ImplTable>;

template <typename Key, typename Mapped, bool ENABLE_PARTITIONED = false,
          typename Hash = DefaultHash<Key>, typename Grower = PartitionedHashTableGrower<>,
          typename Allocator = HashTableAllocator,
          template <typename...> typename ImplTable = HashMapTable>
using PartitionedHashMapWithSavedHash =
        PartitionedHashMapTable<Key, HashMapCellWithSavedHash<Key, Mapped, Hash>,
                                ENABLE_PARTITIONED, Hash, Grower, Allocator, ImplTable>;
