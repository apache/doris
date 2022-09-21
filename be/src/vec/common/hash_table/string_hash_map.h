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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/HashTable/StringHashMap.h
// and modified by Doris

#pragma once

#include "vec/common/hash_table/hash_map.h"
#include "vec/common/hash_table/string_hash_table.h"

template <typename Key, typename TMapped>
struct StringHashMapCell : public HashMapCell<Key, TMapped, StringHashTableHash, HashTableNoState> {
    using Base = HashMapCell<Key, TMapped, StringHashTableHash, HashTableNoState>;
    using value_type = typename Base::value_type;
    using Base::Base;
    static constexpr bool need_zero_value_storage = false;
    // external
    const StringRef get_key() const { return to_string_ref(this->value.first); } /// NOLINT
    // internal
    static const Key& get_key(const value_type& value_) { return value_.first; }
};

template <typename TMapped>
struct StringHashMapCell<StringKey16, TMapped>
        : public HashMapCell<StringKey16, TMapped, StringHashTableHash, HashTableNoState> {
    using Base = HashMapCell<StringKey16, TMapped, StringHashTableHash, HashTableNoState>;
    using value_type = typename Base::value_type;
    using Base::Base;
    static constexpr bool need_zero_value_storage = false;
    bool is_zero(const HashTableNoState& state) const { return is_zero(this->value.first, state); }

    // Zero means unoccupied cells in hash table. Use key with last word = 0 as
    // zero keys, because such keys are unrepresentable (no way to encode length).
    static bool is_zero(const StringKey16& key, const HashTableNoState&) { return key.high == 0; }
    void set_zero() { this->value.first.high = 0; }

    // external
    const StringRef get_key() const { return to_string_ref(this->value.first); } /// NOLINT
    // internal
    static const StringKey16& get_key(const value_type& value_) { return value_.first; }
};

template <typename TMapped>
struct StringHashMapCell<StringKey24, TMapped>
        : public HashMapCell<StringKey24, TMapped, StringHashTableHash, HashTableNoState> {
    using Base = HashMapCell<StringKey24, TMapped, StringHashTableHash, HashTableNoState>;
    using value_type = typename Base::value_type;
    using Base::Base;
    static constexpr bool need_zero_value_storage = false;
    bool is_zero(const HashTableNoState& state) const { return is_zero(this->value.first, state); }

    // Zero means unoccupied cells in hash table. Use key with last word = 0 as
    // zero keys, because such keys are unrepresentable (no way to encode length).
    static bool is_zero(const StringKey24& key, const HashTableNoState&) { return key.c == 0; }
    void set_zero() { this->value.first.c = 0; }

    // external
    const StringRef get_key() const { return to_string_ref(this->value.first); } /// NOLINT
    // internal
    static const StringKey24& get_key(const value_type& value_) { return value_.first; }
};

template <typename TMapped>
struct StringHashMapCell<StringRef, TMapped>
        : public HashMapCellWithSavedHash<StringRef, TMapped, StringHashTableHash,
                                          HashTableNoState> {
    using Base =
            HashMapCellWithSavedHash<StringRef, TMapped, StringHashTableHash, HashTableNoState>;
    using value_type = typename Base::value_type;
    using Base::Base;
    static constexpr bool need_zero_value_storage = false;
    // external
    using Base::get_key;
    // internal
    static const StringRef& get_key(const value_type& value_) { return value_.first; }

    template <typename Key>
    StringHashMapCell(const StringHashMapCell<Key, TMapped>& other) {
        assert(need_zero_value_storage == other.need_zero_value_storage);
        this->value.first = other.get_key();
        this->value.second = other.get_second();
    }

    template <typename Key>
    StringHashMapCell& operator=(const StringHashMapCell<Key, TMapped>& other) {
        assert(need_zero_value_storage == other.need_zero_value_storage);
        this->value.first = other.get_key();
        this->value.second = other.get_second();
        return *this;
    }
};

template <typename TMapped, typename Allocator>
struct StringHashMapSubMaps {
    using T0 = StringHashTableEmpty<StringHashMapCell<StringRef, TMapped>>;
    using T1 = HashMapTable<StringKey8, StringHashMapCell<StringKey8, TMapped>, StringHashTableHash,
                            StringHashTableGrower<>, Allocator>;
    using T2 = HashMapTable<StringKey16, StringHashMapCell<StringKey16, TMapped>,
                            StringHashTableHash, StringHashTableGrower<>, Allocator>;
    using T3 = HashMapTable<StringKey24, StringHashMapCell<StringKey24, TMapped>,
                            StringHashTableHash, StringHashTableGrower<>, Allocator>;
    using Ts = HashMapTable<StringRef, StringHashMapCell<StringRef, TMapped>, StringHashTableHash,
                            StringHashTableGrower<>, Allocator>;
};

template <typename TMapped, typename Allocator = HashTableAllocator>
class StringHashMap : public StringHashTable<StringHashMapSubMaps<TMapped, Allocator>> {
public:
    using Key = StringRef;
    using Base = StringHashTable<StringHashMapSubMaps<TMapped, Allocator>>;
    using Self = StringHashMap;
    using LookupResult = typename Base::LookupResult;

    using Base::Base;

    /// Merge every cell's value of current map into the destination map.
    ///  Func should have signature void(Mapped & dst, Mapped & src, bool emplaced).
    ///  Each filled cell in current map will invoke func once. If that map doesn't
    ///  have a key equals to the given cell, a new cell gets emplaced into that map,
    ///  and func is invoked with the third argument emplaced set to true. Otherwise
    ///  emplaced is set to false.
    template <typename Func>
    void ALWAYS_INLINE merge_to_via_emplace(Self& that, Func&& func) {
        if (this->m0.hasZero() && that.m0.hasZero())
            func(that.m0.zero_value()->get_mapped(), this->m0.zero_value()->get_mapped(), false);
        else if (this->m0.hasZero()) {
            that.m0.setHasZero();
            func(that.m0.zero_value()->get_mapped(), this->m0.zero_value()->get_mapped(), true);
        }
        this->m1.merge_to_via_emplace(that.m1, func);
        this->m2.merge_to_via_emplace(that.m2, func);
        this->m3.merge_to_via_emplace(that.m3, func);
        this->ms.merge_to_via_emplace(that.ms, func);
    }

    /// Merge every cell's value of current map into the destination map via find.
    ///  Func should have signature void(Mapped & dst, Mapped & src, bool exist).
    ///  Each filled cell in current map will invoke func once. If that map doesn't
    ///  have a key equals to the given cell, func is invoked with the third argument
    ///  exist set to false. Otherwise exist is set to true.
    template <typename Func>
    void ALWAYS_INLINE merge_to_via_find(Self& that, Func&& func) {
        if (this->m0.size() && that.m0.size())
            func(that.m0.zero_value()->get_mapped(), this->m0.zero_value()->get_mapped(), true);
        else if (this->m0.size())
            func(this->m0.zero_value()->get_mapped(), this->m0.zero_value()->get_mapped(), false);
        this->m1.merge_to_via_find(that.m1, func);
        this->m2.merge_to_via_find(that.m2, func);
        this->m3.merge_to_via_find(that.m3, func);
        this->ms.merge_to_via_find(that.ms, func);
    }

    TMapped& ALWAYS_INLINE operator[](const Key& x) {
        LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted);
        if (inserted) new (&it->get_mapped()) TMapped();

        return it->get_mapped();
    }

    template <typename Func>
    void ALWAYS_INLINE for_each_value(Func&& func) {
        if (this->m0.size()) {
            func(StringRef {}, this->m0.zero_value()->get_second());
        }

        for (auto& v : this->m1) {
            func(v.get_key(), v.get_second());
        }

        for (auto& v : this->m2) {
            func(v.get_key(), v.get_second());
        }

        for (auto& v : this->m3) {
            func(v.get_key(), v.get_second());
        }

        for (auto& v : this->ms) {
            func(v.get_key(), v.get_second());
        }
    }

    template <typename Func>
    void ALWAYS_INLINE for_each_mapped(Func&& func) {
        if (this->m0.size()) func(this->m0.zero_value()->get_second());
        for (auto& v : this->m1) func(v.get_second());
        for (auto& v : this->m2) func(v.get_second());
        for (auto& v : this->m3) func(v.get_second());
        for (auto& v : this->ms) func(v.get_second());
    }

    char* get_null_key_data() { return nullptr; }
    bool has_null_key_data() const { return false; }
};

template <typename TMapped, typename Allocator>
struct HashTableTraits<StringHashMap<TMapped, Allocator>> {
    static constexpr bool is_phmap = false;
    static constexpr bool is_parallel_phmap = false;
    static constexpr bool is_string_hash_table = true;
};

template <template <typename> class Derived, typename TMapped, typename Allocator>
struct HashTableTraits<Derived<StringHashMap<TMapped, Allocator>>> {
    static constexpr bool is_phmap = false;
    static constexpr bool is_parallel_phmap = false;
    static constexpr bool is_string_hash_table = true;
};
