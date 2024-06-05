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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/HashTable/StringHashTable.h
// and modified by Doris

#pragma once

#include <new>
#include <variant>

#include "vec/common/hash_table/hash.h"
#include "vec/common/hash_table/hash_table.h"
#include "vec/common/memcpy_small.h"

using StringKey2 = doris::vectorized::UInt16;
using StringKey4 = doris::vectorized::UInt32;
using StringKey8 = doris::vectorized::UInt64;
using StringKey16 = doris::vectorized::UInt128;

struct StringHashMapSubKeys {
    using T1 = StringKey2;
    using T2 = StringKey4;
    using T3 = StringKey8;
    using T4 = StringKey16;
};

template <typename StringKey>
StringKey to_string_key(const doris::StringRef& key) {
    DCHECK_LE(key.size, sizeof(StringKey));
    StringKey string_key {};
    memcpy_small<sizeof(StringKey)>((char*)&string_key, key.data, key.size);
    return string_key;
}

template <typename T>
inline doris::StringRef ALWAYS_INLINE to_string_ref(const T& n) {
    assert(n != 0);
    return {reinterpret_cast<const char*>(&n), sizeof(T) - (__builtin_clzll(n) >> 3)};
}
inline doris::StringRef ALWAYS_INLINE to_string_ref(const StringKey16& n) {
    assert(n.items[1] != 0);
    return {reinterpret_cast<const char*>(&n), 16UL - (__builtin_clzll(n.items[1]) >> 3)};
}

struct StringHashTableHash {
#if defined(__SSE4_2__) || defined(__aarch64__)
    template <typename T>
    size_t ALWAYS_INLINE operator()(T key) const {
        size_t res = -1ULL;
        res = _mm_crc32_u64(res, key);
        return res;
    }
    size_t ALWAYS_INLINE operator()(StringKey16 key) const {
        size_t res = -1ULL;
        res = _mm_crc32_u64(res, key.low());
        res = _mm_crc32_u64(res, key.high());
        return res;
    }
#else
    template <typename T>
    size_t ALWAYS_INLINE operator()(T key) const {
        return util_hash::CityHash64(reinterpret_cast<const char*>(&key), sizeof(T));
    }
#endif
    size_t ALWAYS_INLINE operator()(doris::StringRef key) const {
        if (key.size == 0) {
            return 0;
        } else if (key.size <= sizeof(StringHashMapSubKeys::T1)) {
            return StringHashTableHash()(to_string_key<StringHashMapSubKeys::T1>(key));
        } else if (key.size <= sizeof(StringHashMapSubKeys::T2)) {
            return StringHashTableHash()(to_string_key<StringHashMapSubKeys::T2>(key));
        } else if (key.size <= sizeof(StringHashMapSubKeys::T3)) {
            return StringHashTableHash()(to_string_key<StringHashMapSubKeys::T3>(key));
        } else if (key.size <= sizeof(StringHashMapSubKeys::T4)) {
            return StringHashTableHash()(to_string_key<StringHashMapSubKeys::T4>(key));
        }
        return doris::StringRefHash()(key);
    }
};

template <typename Cell>
struct StringHashTableEmpty {
    using Self = StringHashTableEmpty;

    bool _has_zero = false;
    std::aligned_storage_t<sizeof(Cell), alignof(Cell)>
            zero_value_storage; /// Storage of element with zero key.

public:
    bool has_zero() const { return _has_zero; }

    void set_has_zero(const typename Cell::key_type& key) {
        _has_zero = true;
        new (zero_value()) Cell();
        zero_value()->value.first = key;
    }

    void set_has_zero(const Cell& other) {
        _has_zero = true;
        new (zero_value()) Cell(other);
    }

    void clear_has_zero() {
        _has_zero = false;
        if (!std::is_trivially_destructible_v<Cell>) {
            zero_value()->~Cell();
        }
    }

    Cell* zero_value() { return std::launder(reinterpret_cast<Cell*>(&zero_value_storage)); }
    const Cell* zero_value() const {
        return std::launder(reinterpret_cast<const Cell*>(&zero_value_storage));
    }

    using LookupResult = Cell*;
    using ConstLookupResult = const Cell*;

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder&& key, LookupResult& it, bool& inserted, size_t = 0) {
        if (!has_zero()) {
            set_has_zero(key);
            inserted = true;
        } else {
            inserted = false;
        }
        it = zero_value();
    }

    class Constructor {
    public:
        friend struct StringHashTableEmpty;
        template <typename... Args>
        void operator()(Args&&... args) const {
            new (_cell) Cell(std::forward<Args>(args)...);
        }

    private:
        Constructor(Cell* cell) : _cell(cell) {}
        Cell* _cell = nullptr;
    };

    template <typename KeyHolder, typename Func, typename Origin>
    void ALWAYS_INLINE lazy_emplace_with_origin(KeyHolder&& key, Origin&& origin, LookupResult& it,
                                                size_t hash_value, Func&& f) {
        if (!has_zero()) {
            set_has_zero(key);
            std::forward<Func>(f)(Constructor(zero_value()), key, origin);
        }
        it = zero_value();
    }

    template <typename Key>
    LookupResult ALWAYS_INLINE find(const Key&, size_t = 0) {
        return has_zero() ? zero_value() : nullptr;
    }

    template <typename Key>
    ConstLookupResult ALWAYS_INLINE find(const Key&, size_t = 0) const {
        return has_zero() ? zero_value() : nullptr;
    }

    size_t size() const { return has_zero() ? 1 : 0; }
    bool empty() const { return !has_zero(); }
    size_t get_buffer_size_in_bytes() const { return sizeof(Cell); }
};

template <size_t initial_size_degree = 8>
struct StringHashTableGrower : public HashTableGrowerWithPrecalculation<initial_size_degree> {
    // Smooth growing for string maps
    void increase_size() { this->increase_size_degree(1); }
};

template <typename Mapped>
struct StringHashTableLookupResult {
    Mapped* mapped_ptr;
    StringHashTableLookupResult() : mapped_ptr(nullptr) {}
    StringHashTableLookupResult(Mapped* mapped_ptr_) : mapped_ptr(mapped_ptr_) {}
    StringHashTableLookupResult(std::nullptr_t) {}
    auto& get_mapped() { return *mapped_ptr; }
    auto& operator*() { return *this; }
    auto& operator*() const { return *this; }
    auto* operator->() { return this; }
    auto* operator->() const { return this; }
    explicit operator bool() const { return mapped_ptr; }
    friend bool operator==(const StringHashTableLookupResult& a, const std::nullptr_t&) {
        return !a.mapped_ptr;
    }
    friend bool operator==(const std::nullptr_t&, const StringHashTableLookupResult& b) {
        return !b.mapped_ptr;
    }
    friend bool operator!=(const StringHashTableLookupResult& a, const std::nullptr_t&) {
        return a.mapped_ptr;
    }
    friend bool operator!=(const std::nullptr_t&, const StringHashTableLookupResult& b) {
        return b.mapped_ptr;
    }
};

template <typename Mapped>
ALWAYS_INLINE inline auto lookup_result_get_mapped(StringHashTableLookupResult<Mapped> cell) {
    return &cell.get_mapped();
}

template <typename SubMaps>
class StringHashTable : private boost::noncopyable {
protected:
    // Map for storing empty string
    using T0 = typename SubMaps::T0;

    // Short strings are stored as numbers
    using T1 = typename SubMaps::T1;
    using T2 = typename SubMaps::T2;
    using T3 = typename SubMaps::T3;
    using T4 = typename SubMaps::T4;

    // Long strings are stored as doris::StringRef along with saved hash
    using Ts = typename SubMaps::Ts;
    using Self = StringHashTable;

    T0 m0;
    T1 m1;
    T2 m2;
    T3 m3;
    T4 m4;
    Ts ms;

    using Cell = typename Ts::cell_type;

    template <typename Derived, bool is_const>
    class iterator_base {
        using Container = std::conditional_t<is_const, const Self, Self>;

        Container* container = nullptr;
        int sub_table_index;
        typename T1::iterator iterator1;
        typename T2::iterator iterator2;
        typename T3::iterator iterator3;
        typename T4::iterator iterator4;
        typename Ts::iterator iterator5;

        typename Ts::cell_type cell;

        friend class StringHashTable;

    public:
        iterator_base() = default;
        iterator_base(Container* container_, bool end = false) : container(container_) {
            if (end) {
                sub_table_index = 5;
                iterator5 = container->ms.end();
            } else {
                sub_table_index = 0;
                if (container->m0.size() == 0) {
                    sub_table_index++;
                } else {
                    return;
                }

                iterator1 = container->m1.begin();
                if (iterator1 == container->m1.end()) {
                    sub_table_index++;
                } else {
                    return;
                }

                iterator2 = container->m2.begin();
                if (iterator2 == container->m2.end()) {
                    sub_table_index++;
                } else {
                    return;
                }

                iterator3 = container->m3.begin();
                if (iterator3 == container->m3.end()) {
                    sub_table_index++;
                } else {
                    return;
                }

                iterator4 = container->m4.begin();
                if (iterator4 == container->m4.end()) {
                    sub_table_index++;
                } else {
                    return;
                }

                iterator5 = container->ms.begin();
            }
        }

        bool operator==(const iterator_base& rhs) const {
            if (sub_table_index != rhs.sub_table_index) {
                return false;
            }
            switch (sub_table_index) {
            case 0: {
                return true;
            }
            case 1: {
                return iterator1 == rhs.iterator1;
            }
            case 2: {
                return iterator2 == rhs.iterator2;
            }
            case 3: {
                return iterator3 == rhs.iterator3;
            }
            case 4: {
                return iterator4 == rhs.iterator4;
            }
            case 5: {
                return iterator5 == rhs.iterator5;
            }
            }
            LOG(FATAL) << "__builtin_unreachable";
            __builtin_unreachable();
        }

        bool operator!=(const iterator_base& rhs) const { return !(*this == rhs); }

        Derived& operator++() {
            bool need_switch_to_next = false;
            switch (sub_table_index) {
            case 0: {
                need_switch_to_next = true;
                break;
            }
            case 1: {
                ++iterator1;
                if (iterator1 == container->m1.end()) {
                    need_switch_to_next = true;
                }
                break;
            }
            case 2: {
                ++iterator2;
                if (iterator2 == container->m2.end()) {
                    need_switch_to_next = true;
                }
                break;
            }
            case 3: {
                ++iterator3;
                if (iterator3 == container->m3.end()) {
                    need_switch_to_next = true;
                }
                break;
            }
            case 4: {
                ++iterator4;
                if (iterator4 == container->m4.end()) {
                    need_switch_to_next = true;
                }
                break;
            }
            case 5: {
                ++iterator5;
                break;
            }
            }

            while (need_switch_to_next) {
                sub_table_index++;
                need_switch_to_next = false;
                switch (sub_table_index) {
                case 1: {
                    iterator1 = container->m1.begin();
                    if (iterator1 == container->m1.end()) {
                        need_switch_to_next = true;
                    }
                    break;
                }
                case 2: {
                    iterator2 = container->m2.begin();
                    if (iterator2 == container->m2.end()) {
                        need_switch_to_next = true;
                    }
                    break;
                }
                case 3: {
                    iterator3 = container->m3.begin();
                    if (iterator3 == container->m3.end()) {
                        need_switch_to_next = true;
                    }
                    break;
                }
                case 4: {
                    iterator4 = container->m4.begin();
                    if (iterator4 == container->m4.end()) {
                        need_switch_to_next = true;
                    }
                    break;
                }
                case 5: {
                    iterator5 = container->ms.begin();
                    break;
                }
                }
            }

            return static_cast<Derived&>(*this);
        }

        auto& operator*() const {
            switch (sub_table_index) {
            case 0: {
                const_cast<iterator_base*>(this)->cell = *(container->m0.zero_value());
                break;
            }
            case 1: {
                const_cast<iterator_base*>(this)->cell = *iterator1;
                break;
            }
            case 2: {
                const_cast<iterator_base*>(this)->cell = *iterator2;
                break;
            }
            case 3: {
                const_cast<iterator_base*>(this)->cell = *iterator3;
                break;
            }
            case 4: {
                const_cast<iterator_base*>(this)->cell = *iterator4;
                break;
            }
            case 5: {
                const_cast<iterator_base*>(this)->cell = *iterator5;
                break;
            }
            }
            return cell;
        }
        auto* operator->() const { return &(this->operator*()); }

        auto get_ptr() const { return &(this->operator*()); }

        size_t get_hash() const {
            switch (sub_table_index) {
            case 0: {
                return container->m0.zero_value()->get_hash(container->m0);
            }
            case 1: {
                return iterator1->get_hash(container->m1);
            }
            case 2: {
                return iterator2->get_hash(container->m2);
            }
            case 3: {
                return iterator3->get_hash(container->m3);
            }
            case 4: {
                return iterator4->get_hash(container->m4);
            }
            case 5: {
                return iterator5->get_hash(container->ms);
            }
            }
        }

        /**
          * A hack for HashedDictionary.
          *
          * The problem: std-like find() returns an iterator, which has to be
          * compared to end(). On the other hand, HashMap::find() returns
          * LookupResult, which is compared to nullptr. HashedDictionary has to
          * support both hash maps with the same code, hence the need for this
          * hack.
          *
          * The proper way would be to remove iterator interface from our
          * HashMap completely, change all its users to the existing internal
          * iteration interface, and redefine end() to return LookupResult for
          * compatibility with std find(). Unfortunately, now is not the time to
          * do this.
          */
        operator Cell*() const { return nullptr; }
    };

public:
    using Key = doris::StringRef;
    using key_type = Key;
    using mapped_type = typename Ts::mapped_type;
    using value_type = typename Ts::value_type;
    using cell_type = typename Ts::cell_type;

    using LookupResult = StringHashTableLookupResult<typename cell_type::mapped_type>;
    using ConstLookupResult = StringHashTableLookupResult<const typename cell_type::mapped_type>;

    StringHashTable() = default;

    explicit StringHashTable(size_t reserve_for_num_elements)
            : m1 {reserve_for_num_elements / 5},
              m2 {reserve_for_num_elements / 5},
              m3 {reserve_for_num_elements / 5},
              m4 {reserve_for_num_elements / 5},
              ms {reserve_for_num_elements / 5} {}

    ~StringHashTable() = default;

    size_t hash(const doris::StringRef& key) { return StringHashTableHash()(key); }

    // Dispatch is written in a way that maximizes the performance:
    // 1. Always memcpy 8 times bytes
    // 2. Use switch case extension to generate fast dispatching table
    // 3. Funcs are named callables that can be force_inlined
    //
    // NOTE: It relies on Little Endianness
    //
    // NOTE: It requires padded to 8 bytes keys (IOW you cannot pass
    // std::string here, but you can pass i.e. ColumnString::getDataAt()),
    // since it copies 8 bytes at a time.
    template <typename Self, typename KeyHolder, typename Func>
    static auto ALWAYS_INLINE dispatch(Self& self, KeyHolder&& key, size_t hash_value,
                                       Func&& func) {
        const size_t sz = key.size;
        if (sz == 0) {
            return func(self.m0, std::forward<KeyHolder>(key), key, 0);
        }

        if (key.data[sz - 1] == 0) {
            // Strings with trailing zeros are not representable as fixed-size
            // string keys. Put them to the generic table.
            return func(self.ms, std::forward<KeyHolder>(key), key, hash_value);
        }

        if (sz <= sizeof(StringHashMapSubKeys::T1)) {
            return func(self.m1, to_string_key<StringHashMapSubKeys::T1>(key), key, hash_value);
        }
        if (sz <= sizeof(StringHashMapSubKeys::T2)) {
            return func(self.m2, to_string_key<StringHashMapSubKeys::T2>(key), key, hash_value);
        }
        if (sz <= sizeof(StringHashMapSubKeys::T3)) {
            return func(self.m3, to_string_key<StringHashMapSubKeys::T3>(key), key, hash_value);
        }
        if (sz <= sizeof(StringHashMapSubKeys::T4)) {
            return func(self.m4, to_string_key<StringHashMapSubKeys::T4>(key), key, hash_value);
        }

        return func(self.ms, std::forward<KeyHolder>(key), key, hash_value);
    }

    struct EmplaceCallable {
        LookupResult& mapped;
        bool& inserted;

        EmplaceCallable(LookupResult& mapped_, bool& inserted_)
                : mapped(mapped_), inserted(inserted_) {}

        template <typename Map, typename KeyHolder, typename Origin>
        void ALWAYS_INLINE operator()(Map& map, KeyHolder&& key_holder, Origin&& origin,
                                      size_t hash) {
            typename Map::LookupResult result;
            map.emplace(key_holder, result, inserted, hash);
            mapped = &result->get_second();
        }
    };

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder&& key_holder, LookupResult& it, bool& inserted,
                               size_t hash_value) {
        this->dispatch(*this, key_holder, hash_value, EmplaceCallable(it, inserted));
    }

    template <typename Func>
    struct LazyEmplaceCallable {
        LookupResult& mapped;
        Func&& f;

        LazyEmplaceCallable(LookupResult& mapped_, Func&& f_)
                : mapped(mapped_), f(std::forward<Func>(f_)) {}

        template <typename Map, typename Key, typename OriginalKey>
        void ALWAYS_INLINE operator()(Map& map, Key&& key, OriginalKey&& origin, size_t hash) {
            typename Map::LookupResult result;
            map.lazy_emplace_with_origin(key, origin, result, hash, std::forward<Func>(f));
            mapped = &result->get_second();
        }
    };

    template <typename KeyHolder, typename Func>
    void ALWAYS_INLINE lazy_emplace(KeyHolder&& key_holder, LookupResult& it, size_t hash_value,
                                    Func&& f) {
        this->dispatch(*this, key_holder, hash_value,
                       LazyEmplaceCallable<Func>(it, std::forward<Func>(f)));
    }

    template <bool read>
    void ALWAYS_INLINE prefetch(const doris::StringRef& key, size_t hash_value) {
        if (!key.size) {
            return;
        }
        if (key.size <= sizeof(StringHashMapSubKeys::T1)) {
            m1.template prefetch<read>(hash_value);
        } else if (key.size <= sizeof(StringHashMapSubKeys::T2)) {
            m2.template prefetch<read>(hash_value);
        } else if (key.size <= sizeof(StringHashMapSubKeys::T3)) {
            m3.template prefetch<read>(hash_value);
        } else if (key.size <= sizeof(StringHashMapSubKeys::T4)) {
            m4.template prefetch<read>(hash_value);
        } else {
            ms.template prefetch<read>(hash_value);
        }
    }

    struct FindCallable {
        // find() doesn't need any key memory management, so we don't work with
        // any key holders here, only with normal keys. The key type is still
        // different for every subtable, this is why it is a template parameter.
        template <typename Submap, typename SubmapKey, typename Origin>
        auto ALWAYS_INLINE operator()(Submap& map, const SubmapKey& key, const Origin& origin,
                                      size_t hash) {
            auto it = map.find(key, hash);
            if (!it) {
                return decltype(&it->get_mapped()) {};
            } else {
                return &it->get_mapped();
            }
        }
    };

    LookupResult ALWAYS_INLINE find(const Key& x, size_t hash_value) {
        return dispatch(*this, x, hash_value, FindCallable {});
    }

    ConstLookupResult ALWAYS_INLINE find(const Key& x, size_t hash_value) const {
        return dispatch(*this, x, hash_value, FindCallable {});
    }

    size_t size() const {
        return m0.size() + m1.size() + m2.size() + m3.size() + m4.size() + ms.size();
    }

    bool empty() const {
        return m0.empty() && m1.empty() && m2.empty() && m3.empty() && m4.empty() && ms.empty();
    }

    size_t get_buffer_size_in_bytes() const {
        return m0.get_buffer_size_in_bytes() + m1.get_buffer_size_in_bytes() +
               m2.get_buffer_size_in_bytes() + m3.get_buffer_size_in_bytes() +
               ms.get_buffer_size_in_bytes();
    }

    class iterator : public iterator_base<iterator, false> {
    public:
        using iterator_base<iterator, false>::iterator_base;
    };

    class const_iterator : public iterator_base<const_iterator, true> {
    public:
        using iterator_base<const_iterator, true>::iterator_base;
    };

    const_iterator begin() const { return const_iterator(this); }

    const_iterator cbegin() const { return begin(); }

    iterator begin() { return iterator(this); }

    const_iterator end() const { return const_iterator(this, true); }
    const_iterator cend() const { return end(); }
    iterator end() { return iterator(this, true); }

    bool add_elem_size_overflow(size_t add_size) const {
        return m1.add_elem_size_overflow(add_size) || m2.add_elem_size_overflow(add_size) ||
               m3.add_elem_size_overflow(add_size) || m4.add_elem_size_overflow(add_size) ||
               ms.add_elem_size_overflow(add_size);
    }
};
