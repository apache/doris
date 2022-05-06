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

#include "hash_table.h"

namespace doris::vectorized {

/** HashTableProxyForMultiTables contains (1 << bits_for_tables_num) tables.
  */
template <typename TData, size_t bits_for_tables_num = 2>
class HashTableProxyForMultiTables {
public:
    using Data = TData;
    using Self = HashTableProxyForMultiTables;
    using LookupResult = typename Data::LookupResult;
    using key_type = typename Data::key_type;
    using mapped_type = typename Data::mapped_type;
    using value_type = typename Data::value_type;

    static constexpr size_t tables_num = 1 << bits_for_tables_num;
    static constexpr size_t max_hash_value_shift = std::max(bits_for_tables_num, size_t(32));

    HashTableProxyForMultiTables() { tail_table = &tables[tables_num - 1]; }

private:
    Data tables[tables_num];
    Data* tail_table;

    template <typename Derived, bool is_const>
    class iterator_base {
    public:
        using it_type = std::conditional_t<is_const, typename Data::const_iterator,
                                           typename Data::iterator>;

    private:
        using Container = std::conditional_t<is_const, const Self, Self>;

        Container* container;
        Data* container_proxy;
        it_type it;

        friend class HashTableProxyForMultiTables;

    public:
        iterator_base() {}
        iterator_base(Container* container_, Data* current_data, it_type it_)
                : container(container_), container_proxy(current_data), it(it_) {}

        bool operator==(const iterator_base& rhs) const {
            return it == rhs.it && container_proxy == rhs.container_proxy;
        }

        bool operator!=(const iterator_base& rhs) const {
            return it != rhs.it || container_proxy != rhs.container_proxy;
        }

        Derived& operator++() {
            ++it;
            while (it == container_proxy->end() && container_proxy != container->tail_table) {
                container_proxy++;
                it = container_proxy->begin();
            }
            return static_cast<Derived&>(*this);
        }

        auto& operator*() const { return *it; }
        auto* operator->() const { return it.get_ptr(); }

        auto get_ptr() const { return it.get_ptr(); }
        size_t get_hash() const { return it->get_hash(container_proxy); }

        size_t get_collision_chain_length() const {
            size_t length = 0;
            for (const auto& table : container->tables) {
                length += table.get_collision_chain_length();
            }
            return length;
        }
    };

    static size_t ALWAYS_INLINE get_table_index_by_hash_value(size_t hash_value) {
        return (hash_value >> (32 - max_hash_value_shift)) & (tables_num - 1);
    }

public:
    bool add_elem_size_overflow(size_t add_size) const {
        for (auto& table : tables)
            if (!table.add_elem_size_overflow(add_size)) return false;
        return true;
    }

    size_t get_buffer_size_in_bytes() const {
        size_t size = 0;
        for (const auto& table : tables) size += table.get_buffer_size_in_bytes();
        return size;
    }

    size_t size() const {
        size_t size = 0;
        for (const auto& table : tables) size = std::max(table.size(), size);
        return size;
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

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder&& key_holder, LookupResult& it, bool& inserted) {
        auto key = key_holder_get_key(key_holder);
        auto hash_value = tables[0].hash(key);
        tables[get_table_index_by_hash_value(hash_value)].emplace(key_holder, it, inserted,
                                                                  hash_value);
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder&& key_holder, LookupResult& it, bool& inserted,
                               size_t hash_value) {
        tables[get_table_index_by_hash_value(hash_value)].emplace(key_holder, it, inserted,
                                                                  hash_value);
    }

    class iterator : public iterator_base<iterator, false> {
    public:
        using iterator_base<iterator, false>::iterator_base;
        using it_type = typename iterator_base<iterator, false>::it_type;
    };

    class const_iterator : public iterator_base<const_iterator, true> {
    public:
        using iterator_base<const_iterator, true>::iterator_base;
        using it_type = typename iterator_base<const_iterator, false>::it_type;
    };

    const_iterator begin() const {
        for (size_t i = 0; i < tables_num; i++) {
            auto it = tables[i].begin();
            Data* current = &tables[i];

            if (it != current->end()) return const_iterator(this, current, it);
        }
        return const_iterator(this, tail_table, tail_table->begin());
    }

    const_iterator cbegin() const { return begin(); }

    iterator begin() {
        for (size_t i = 0; i < tables_num; i++) {
            typename iterator::it_type it = tables[i].begin();
            Data* current = &tables[i];

            if (it != current->end()) return iterator(this, current, it);
        }
        return iterator(this, tail_table, tail_table->begin());
    }

    const_iterator end() const { return const_iterator(this, tail_table, tail_table->end()); }
    const_iterator cend() const { return end(); }
    iterator end() { return iterator(this, tail_table, tail_table->end()); }

    char* get_null_key_data() { return nullptr; }
    bool has_null_key_data() const { return false; }
};

} // namespace doris::vectorized
