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

namespace doris {
template <typename Key, typename TMapped>
struct StringHashMapCell : public HashMapCell<Key, TMapped, StringHashTableHash, HashTableNoState> {
    using Base = HashMapCell<Key, TMapped, StringHashTableHash, HashTableNoState>;
    using value_type = typename Base::value_type;
    using Base::Base;
    static constexpr bool need_zero_value_storage = false;
    // external
    const doris::StringRef get_key() const { return to_string_ref(this->value.first); } /// NOLINT
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
    static bool is_zero(const StringKey16& key, const HashTableNoState&) {
        return key.items[1] == 0;
    }
    void set_zero() { this->value.first.items[1] = 0; }

    // external
    const doris::StringRef get_key() const { return to_string_ref(this->value.first); } /// NOLINT
    // internal
    static const StringKey16& get_key(const value_type& value_) { return value_.first; }
};

template <typename TMapped>
struct StringHashMapCell<doris::StringRef, TMapped>
        : public HashMapCellWithSavedHash<doris::StringRef, TMapped, StringHashTableHash,
                                          HashTableNoState> {
    using Base = HashMapCellWithSavedHash<doris::StringRef, TMapped, StringHashTableHash,
                                          HashTableNoState>;
    using value_type = typename Base::value_type;
    using Base::Base;
    static constexpr bool need_zero_value_storage = false;
    // external
    using Base::get_key;
    // internal
    static const doris::StringRef& get_key(const value_type& value_) { return value_.first; }

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
    using T0 = StringHashTableEmpty<StringHashMapCell<doris::StringRef, TMapped>>;
    using T1 = HashMapTable<StringHashMapSubKeys::T1,
                            StringHashMapCell<StringHashMapSubKeys::T1, TMapped>,
                            StringHashTableHash, StringHashTableGrower<4>, Allocator>;
    using T2 = HashMapTable<StringHashMapSubKeys::T2,
                            StringHashMapCell<StringHashMapSubKeys::T2, TMapped>,
                            StringHashTableHash, StringHashTableGrower<>, Allocator>;
    using T3 = HashMapTable<StringHashMapSubKeys::T3,
                            StringHashMapCell<StringHashMapSubKeys::T3, TMapped>,
                            StringHashTableHash, StringHashTableGrower<>, Allocator>;
    using T4 = HashMapTable<StringHashMapSubKeys::T4,
                            StringHashMapCell<StringHashMapSubKeys::T4, TMapped>,
                            StringHashTableHash, StringHashTableGrower<>, Allocator>;
    using Ts = HashMapTable<doris::StringRef, StringHashMapCell<doris::StringRef, TMapped>,
                            StringHashTableHash, StringHashTableGrower<>, Allocator>;
};

template <typename TMapped, typename Allocator = HashTableAllocator>
class StringHashMap : public StringHashTable<StringHashMapSubMaps<TMapped, Allocator>> {
public:
    using Key = doris::StringRef;
    using Base = StringHashTable<StringHashMapSubMaps<TMapped, Allocator>>;
    using Self = StringHashMap;
    using LookupResult = typename Base::LookupResult;

    using Base::Base;

    TMapped& ALWAYS_INLINE operator[](const Key& x) {
        LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted);
        if (inserted) {
            new (&it->get_mapped()) TMapped();
        }

        return it->get_mapped();
    }

    template <typename Func>
    void ALWAYS_INLINE for_each_mapped(Func&& func) {
        if (this->m0.size()) {
            func(this->m0.zero_value()->get_second());
        }
        for (auto& v : this->m1) {
            func(v.get_second());
        }
        for (auto& v : this->m2) {
            func(v.get_second());
        }
        for (auto& v : this->m3) {
            func(v.get_second());
        }
        for (auto& v : this->m4) {
            func(v.get_second());
        }
        for (auto& v : this->ms) {
            func(v.get_second());
        }
    }
    template <typename MappedType>
    char* get_null_key_data() {
        return nullptr;
    }
    bool has_null_key_data() const { return false; }
};
} // namespace doris