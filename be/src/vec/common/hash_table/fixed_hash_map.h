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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/HashTable/FixedHashMap.h
// and modified by Doris

#pragma once

#include "vec/common/hash_table/fixed_hash_table.h"
#include "vec/common/hash_table/hash_map.h"

template <typename Key, typename TMapped, typename TState = HashTableNoState>
struct FixedHashMapCell {
    using Mapped = TMapped;
    using State = TState;

    using value_type = PairNoInit<Key, Mapped>;
    using mapped_type = TMapped;

    bool full;
    Mapped mapped;

    FixedHashMapCell() {}
    FixedHashMapCell(const Key&, const State&) : full(true) {}
    FixedHashMapCell(const value_type& value_, const State&) : full(true), mapped(value_.second) {}

    const VoidKey get_key() const { return {}; }
    Mapped& get_mapped() { return mapped; }
    const Mapped& get_mapped() const { return mapped; }

    bool is_zero(const State&) const { return !full; }
    void set_zero() { full = false; }

    /// Similar to FixedHashSetCell except that we need to contain a pointer to the Mapped field.
    ///  Note that we have to assemble a continuous layout for the value_type on each call of getValue().
    struct CellExt {
        CellExt() {}
        CellExt(Key&& key_, const FixedHashMapCell* ptr_)
                : key(key_), ptr(const_cast<FixedHashMapCell*>(ptr_)) {}
        void update(Key&& key_, const FixedHashMapCell* ptr_) {
            key = key_;
            ptr = const_cast<FixedHashMapCell*>(ptr_);
        }
        Key key;
        FixedHashMapCell* ptr;

        const Key& get_key() const { return key; }
        Mapped& get_mapped() { return ptr->mapped; }
        const Mapped& get_mapped() const { return ptr->mapped; }
        const value_type get_value() const { return {key, ptr->mapped}; }
    };
};

/// In case when we can encode empty cells with zero mapped values.
template <typename Key, typename TMapped, typename TState = HashTableNoState>
struct FixedHashMapImplicitZeroCell {
    using Mapped = TMapped;
    using State = TState;

    using value_type = PairNoInit<Key, Mapped>;
    using mapped_type = TMapped;

    Mapped mapped;

    FixedHashMapImplicitZeroCell() {}
    FixedHashMapImplicitZeroCell(const Key&, const State&) {}
    FixedHashMapImplicitZeroCell(const Key&, const Mapped& mapped_) : mapped(mapped_) {}
    FixedHashMapImplicitZeroCell(const value_type& value_, const State&) : mapped(value_.second) {}

    const VoidKey get_first() const { return {}; }
    Mapped& get_second() { return mapped; }
    const Mapped& get_second() const { return mapped; }

    bool is_zero(const State&) const { return !mapped; }
    void set_zero() { mapped = {}; }

    /// Similar to FixedHashSetCell except that we need to contain a pointer to the Mapped field.
    ///  Note that we have to assemble a continuous layout for the value_type on each call of getValue().
    struct CellExt {
        CellExt() {}
        CellExt(Key&& key_, const FixedHashMapImplicitZeroCell* ptr_)
                : key(key_), ptr(const_cast<FixedHashMapImplicitZeroCell*>(ptr_)) {}
        void update(Key&& key_, const FixedHashMapImplicitZeroCell* ptr_) {
            key = key_;
            ptr = const_cast<FixedHashMapImplicitZeroCell*>(ptr_);
        }
        Key key;
        FixedHashMapImplicitZeroCell* ptr;

        const Key& get_first() const { return key; }
        Mapped& get_second() { return ptr->mapped; }
        const Mapped& get_second() const { return ptr->mapped; }
        const value_type get_value() const { return {key, ptr->mapped}; }
    };
};

template <typename Key, typename Mapped, typename State>
ALWAYS_INLINE inline auto lookup_result_get_mapped(
        FixedHashMapImplicitZeroCell<Key, Mapped, State>* cell) {
    return &cell->get_second();
}

template <typename Key, typename Mapped, typename Cell = FixedHashMapCell<Key, Mapped>,
          typename Size = FixedHashTableStoredSize<Cell>, typename Allocator = HashTableAllocator>
class FixedHashMap : public FixedHashTable<Key, Cell, Size, Allocator> {
public:
    using Base = FixedHashTable<Key, Cell, Size, Allocator>;
    using Self = FixedHashMap;
    using LookupResult = typename Base::LookupResult;

    using Base::Base;

    template <typename Func>
    void for_each_value(Func&& func) {
        for (auto& v : *this) func(v.get_key(), v.get_mapped());
    }

    template <typename Func>
    void for_each_mapped(Func&& func) {
        for (auto& v : *this) func(v.get_second());
    }

    Mapped& ALWAYS_INLINE operator[](const Key& x) {
        LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted);
        if (inserted) new (&it->get_mapped()) Mapped();

        return it->get_mapped();
    }

    // fixed hash map never overflow
    bool add_elem_size_overflow(size_t add_size) const { return false; }
    template <typename MappedType>
    char* get_null_key_data() {
        return nullptr;
    }
    bool has_null_key_data() const { return false; }
};

template <typename Key, typename Mapped, typename Allocator = HashTableAllocator>
using FixedImplicitZeroHashMap =
        FixedHashMap<Key, Mapped, FixedHashMapImplicitZeroCell<Key, Mapped>,
                     FixedHashTableStoredSize<FixedHashMapImplicitZeroCell<Key, Mapped>>,
                     Allocator>;

template <typename Key, typename Mapped, typename Allocator = HashTableAllocator>
using FixedImplicitZeroHashMapWithCalculatedSize =
        FixedHashMap<Key, Mapped, FixedHashMapImplicitZeroCell<Key, Mapped>,
                     FixedHashTableCalculatedSize<FixedHashMapImplicitZeroCell<Key, Mapped>>,
                     Allocator>;