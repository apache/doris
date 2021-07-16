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

#include "vec/common/hash_table/hash.h"
#include "vec/common/hash_table/hash_table.h"
#include "vec/common/hash_table/hash_table_allocator.h"

/** NOTE HashSet could only be used for memmoveable (position independent) types.
  * Example: std::string is not position independent in libstdc++ with C++11 ABI or in libc++.
  * Also, key must be of type, that zero bytes is compared equals to zero key.
  */

template <typename Key, typename TCell, typename Hash = DefaultHash<Key>,
          typename Grower = HashTableGrower<>, typename Allocator = HashTableAllocator>
class HashSetTable : public HashTable<Key, TCell, Hash, Grower, Allocator> {
public:
    using Self = HashSetTable;
    using Cell = TCell;

    using Base = HashTable<Key, TCell, Hash, Grower, Allocator>;
    using typename Base::LookupResult;

    void merge(const Self& rhs) {
        if (!this->get_has_zero() && rhs.get_has_zero()) {
            this->set_get_has_zero();
            ++this->m_size;
        }

        for (size_t i = 0; i < rhs.grower.buf_size(); ++i)
            if (!rhs.buf[i].is_zero(*this)) this->insert(Cell::get_key(rhs.buf[i].get_value()));
    }

};

template <typename Key, typename Hash, typename TState = HashTableNoState>
struct HashSetCellWithSavedHash : public HashTableCell<Key, Hash, TState> {
    using Base = HashTableCell<Key, Hash, TState>;

    size_t saved_hash;

    HashSetCellWithSavedHash() : Base() {}
    HashSetCellWithSavedHash(const Key& key_, const typename Base::State& state)
            : Base(key_, state) {}

    bool key_equals(const Key& key_) const { return this->key == key_; }
    bool key_equals(const Key& key_, size_t hash_) const {
        return saved_hash == hash_ && this->key == key_;
    }
    bool key_equals(const Key& key_, size_t hash_, const typename Base::State&) const {
        return key_equals(key_, hash_);
    }

    void set_hash(size_t hash_value) { saved_hash = hash_value; }
    size_t get_hash(const Hash& /*hash_function*/) const { return saved_hash; }
};

template <typename Key, typename Hash, typename State>
ALWAYS_INLINE inline auto lookup_result_get_key(HashSetCellWithSavedHash<Key, Hash, State>* cell) {
    return &cell->key;
}

template <typename Key, typename Hash, typename State>
ALWAYS_INLINE inline void* lookup_result_get_mapped(HashSetCellWithSavedHash<Key, Hash, State>*) {
    return nullptr;
}

template <typename Key, typename Hash = DefaultHash<Key>, typename Grower = HashTableGrower<>,
          typename Allocator = HashTableAllocator>
using HashSet = HashSetTable<Key, HashTableCell<Key, Hash>, Hash, Grower, Allocator>;

template <typename Key, typename Hash = DefaultHash<Key>, typename Grower = HashTableGrower<>,
          typename Allocator = HashTableAllocator>
using HashSetWithSavedHash =
        HashSetTable<Key, HashSetCellWithSavedHash<Key, Hash>, Hash, Grower, Allocator>;

template <typename Key, typename Hash, size_t initial_size_degree>
using HashSetWithStackMemory =
        HashSet<Key, Hash, HashTableGrower<initial_size_degree>,
                HashTableAllocatorWithStackMemory<(1ULL << initial_size_degree) *
                                                  sizeof(HashTableCell<Key, Hash>)>>;

template <typename Key, typename Hash, size_t initial_size_degree>
using HashSetWithSavedHashWithStackMemory = HashSetWithSavedHash<
        Key, Hash, HashTableGrower<initial_size_degree>,
        HashTableAllocatorWithStackMemory<(1ULL << initial_size_degree) *
                                          sizeof(HashSetCellWithSavedHash<Key, Hash>)>>;