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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/HashTable/TwoLevelHashTable.h
// and modified by Doris
#pragma once

#include "vec/common/hash_table/hash_table.h"
#include "vec/common/hash_table/hash_table_utils.h"

/** Partitioned hash table.
  * Represents 16 (or 1ULL << BITS_FOR_SUB_TABLE) small hash tables (sub table count of the first level).
  * To determine which one to use, one of the bytes of the hash function is taken.
  *
  * Usually works a little slower than a simple hash table.
  * However, it has advantages in some cases:
  * - if you need to merge two hash tables together, then you can easily parallelize it by sub tables;
  * - delay during resizes is amortized, since the small hash tables will be resized separately;
  * - in theory, resizes are cache-local in a larger range of sizes.
  */

template <size_t initial_size_degree = 8>
struct PartitionedHashTableGrower : public HashTableGrowerWithPrecalculation<initial_size_degree> {
    /// Increase the size of the hash table.
    void increase_size() { this->increase_size_degree(this->size_degree() >= 15 ? 1 : 2); }
};

template <typename Impl, size_t BITS_FOR_SUB_TABLE = 4>
class PartitionedHashTable : private boost::noncopyable, Impl::Hash {
public:
    using key_type = typename Impl::key_type;
    using mapped_type = typename Impl::mapped_type;
    using value_type = typename Impl::value_type;
    using cell_type = typename Impl::cell_type;
    using Key = typename Impl::key_type;

    using LookupResult = typename Impl::LookupResult;
    using ConstLookupResult = typename Impl::ConstLookupResult;

protected:
    using Self = PartitionedHashTable;

private:
    static constexpr size_t NUM_LEVEL1_SUB_TABLES = 1ULL << BITS_FOR_SUB_TABLE;
    static constexpr size_t MAX_SUB_TABLE = NUM_LEVEL1_SUB_TABLES - 1;

    //factor that will trigger growing the hash table on insert.
    static constexpr float MAX_SUB_TABLE_OCCUPANCY_FRACTION = 0.5f;

    Impl level0_sub_table;
    Impl level1_sub_tables[NUM_LEVEL1_SUB_TABLES];

    bool _is_partitioned = false;

    int64_t _convert_timer_ns = 0;

public:
    PartitionedHashTable() = default;

    PartitionedHashTable(PartitionedHashTable&& rhs) { *this = std::move(rhs); }

    PartitionedHashTable& operator=(PartitionedHashTable&& rhs) {
        std::swap(_is_partitioned, rhs._is_partitioned);

        level0_sub_table = std::move(rhs.level0_sub_table);
        for (size_t i = 0; i < NUM_LEVEL1_SUB_TABLES; ++i) {
            level1_sub_tables[i] = std::move(rhs.level1_sub_tables[i]);
        }

        return *this;
    }

    size_t hash(const Key& x) const { return level0_sub_table.hash(x); }

    float get_factor() const { return MAX_SUB_TABLE_OCCUPANCY_FRACTION; }

    int64_t get_convert_timer_value() const { return _convert_timer_ns; }

    bool should_be_shrink(int64_t valid_row) const {
        if (_is_partitioned) {
            return false;
        } else {
            return level0_sub_table.should_be_shrink(valid_row);
        }
    }

    template <typename Func>
    void ALWAYS_INLINE for_each_value(Func&& func) {
        if (_is_partitioned) {
            for (auto i = 0u; i < NUM_LEVEL1_SUB_TABLES; ++i) {
                level1_sub_tables[i].for_each_value(func);
            }
        } else {
            level0_sub_table.for_each_value(func);
        }
    }

    size_t size() {
        size_t count = 0;
        if (_is_partitioned) {
            for (auto i = 0u; i < this->NUM_LEVEL1_SUB_TABLES; ++i) {
                count += this->level1_sub_tables[i].size();
            }
        } else {
            count = level0_sub_table.size();
        }
        return count;
    }

    void init_buf_size(size_t reserve_for_num_elements) {
        if (_is_partitioned) {
            for (auto& impl : level1_sub_tables) {
                impl.init_buf_size(reserve_for_num_elements / NUM_LEVEL1_SUB_TABLES);
            }
        } else {
            if (level0_sub_table.check_if_need_partition(reserve_for_num_elements)) {
                level0_sub_table.clear_and_shrink();
                _is_partitioned = true;

                for (size_t i = 0; i < NUM_LEVEL1_SUB_TABLES; ++i) {
                    level1_sub_tables[i].init_buf_size(reserve_for_num_elements /
                                                       NUM_LEVEL1_SUB_TABLES);
                }
            } else {
                level0_sub_table.init_buf_size(reserve_for_num_elements);
            }
        }
    }

    void delete_zero_key(Key key) {
        if (_is_partitioned) {
            const auto key_hash = hash(key);
            size_t sub_table_idx = get_sub_table_from_hash(key_hash);
            level1_sub_tables[sub_table_idx].delete_zero_key(key);
        } else {
            level0_sub_table.delete_zero_key(key);
        }
    }

    int64_t get_collisions() const {
        size_t collisions = level0_sub_table.get_collisions();
        for (size_t i = 0; i < NUM_LEVEL1_SUB_TABLES; i++) {
            collisions += level1_sub_tables[i].get_collisions();
        }
        return collisions;
    }

    size_t get_buffer_size_in_bytes() const {
        if (_is_partitioned) {
            size_t buff_size = 0;
            for (const auto& impl : level1_sub_tables) buff_size += impl.get_buffer_size_in_bytes();
            return buff_size;
        } else {
            return level0_sub_table.get_buffer_size_in_bytes();
        }
    }

    size_t get_buffer_size_in_cells() const {
        if (_is_partitioned) {
            size_t buff_size = 0;
            for (const auto& impl : level1_sub_tables) buff_size += impl.get_buffer_size_in_cells();
            return buff_size;
        } else {
            return level0_sub_table.get_buffer_size_in_cells();
        }
    }

    std::vector<size_t> get_buffer_sizes_in_cells() const {
        std::vector<size_t> sizes;
        if (_is_partitioned) {
            for (size_t i = 0; i < NUM_LEVEL1_SUB_TABLES; ++i) {
                sizes.push_back(level1_sub_tables[i].get_buffer_size_in_cells());
            }
        } else {
            sizes.push_back(level0_sub_table.get_buffer_size_in_cells());
        }
        return sizes;
    }

    void reset_resize_timer() {
        if (_is_partitioned) {
            for (auto& impl : level1_sub_tables) {
                impl.reset_resize_timer();
            }
        } else {
            level0_sub_table.reset_resize_timer();
        }
    }
    int64_t get_resize_timer_value() const {
        if (_is_partitioned) {
            int64_t resize_timer_ns = 0;
            for (const auto& impl : level1_sub_tables) {
                resize_timer_ns += impl.get_resize_timer_value();
            }
            return resize_timer_ns;
        } else {
            return level0_sub_table.get_resize_timer_value();
        }
    }

    bool has_null_key_data() const { return false; }
    template <typename MappedType>
    char* get_null_key_data() {
        return nullptr;
    }

protected:
    typename Impl::iterator begin_of_next_non_empty_sub_table_idx(size_t& sub_table_idx) {
        while (sub_table_idx != NUM_LEVEL1_SUB_TABLES && level1_sub_tables[sub_table_idx].empty())
            ++sub_table_idx;

        if (sub_table_idx != NUM_LEVEL1_SUB_TABLES) return level1_sub_tables[sub_table_idx].begin();

        --sub_table_idx;
        return level1_sub_tables[MAX_SUB_TABLE].end();
    }

    typename Impl::const_iterator begin_of_next_non_empty_sub_table_idx(
            size_t& sub_table_idx) const {
        while (sub_table_idx != NUM_LEVEL1_SUB_TABLES && level1_sub_tables[sub_table_idx].empty())
            ++sub_table_idx;

        if (sub_table_idx != NUM_LEVEL1_SUB_TABLES) return level1_sub_tables[sub_table_idx].begin();

        --sub_table_idx;
        return level1_sub_tables[MAX_SUB_TABLE].end();
    }

public:
    void set_partitioned_threshold(int threshold) {
        level0_sub_table.set_partitioned_threshold(threshold);
    }

    class iterator /// NOLINT
    {
        Self* container {};
        size_t sub_table_idx {};
        typename Impl::iterator current_it {};

        friend class PartitionedHashTable;

        iterator(Self* container_, size_t sub_table_idx_, typename Impl::iterator current_it_)
                : container(container_), sub_table_idx(sub_table_idx_), current_it(current_it_) {}

    public:
        iterator() = default;

        bool operator==(const iterator& rhs) const {
            return sub_table_idx == rhs.sub_table_idx && current_it == rhs.current_it;
        }
        bool operator!=(const iterator& rhs) const { return !(*this == rhs); }

        iterator& operator++() {
            ++current_it;
            if (container->_is_partitioned) {
                if (current_it == container->level1_sub_tables[sub_table_idx].end()) {
                    ++sub_table_idx;
                    current_it = container->begin_of_next_non_empty_sub_table_idx(sub_table_idx);
                }
            }

            return *this;
        }

        auto& operator*() { return *current_it; }
        auto* operator->() { return current_it.get_ptr(); }

        auto* get_ptr() { return current_it.get_ptr(); }
        size_t get_hash() { return current_it.get_hash(); }
    };

    class const_iterator /// NOLINT
    {
        Self* container {};
        size_t sub_table_idx {};
        typename Impl::const_iterator current_it {};

        friend class PartitionedHashTable;

        const_iterator(Self* container_, size_t sub_table_idx_,
                       typename Impl::const_iterator current_it_)
                : container(container_), sub_table_idx(sub_table_idx_), current_it(current_it_) {}

    public:
        const_iterator() = default;
        const_iterator(const iterator& rhs)
                : container(rhs.container),
                  sub_table_idx(rhs.sub_table_idx),
                  current_it(rhs.current_it) {} /// NOLINT

        bool operator==(const const_iterator& rhs) const {
            return sub_table_idx == rhs.sub_table_idx && current_it == rhs.current_it;
        }
        bool operator!=(const const_iterator& rhs) const { return !(*this == rhs); }

        const_iterator& operator++() {
            ++current_it;
            if (container->_is_partitioned) {
                if (current_it == container->level1_sub_tables[sub_table_idx].end()) {
                    ++sub_table_idx;
                    current_it = container->begin_of_next_non_empty_sub_table_idx(sub_table_idx);
                }
            }

            return *this;
        }

        const auto& operator*() const { return *current_it; }
        const auto* operator->() const { return current_it->get_ptr(); }

        const auto* get_ptr() const { return current_it.get_ptr(); }
        size_t get_hash() const { return current_it.get_hash(); }
    };

    const_iterator begin() const {
        if (_is_partitioned) {
            size_t sub_table_idx = 0;
            typename Impl::const_iterator impl_it =
                    begin_of_next_non_empty_sub_table_idx(sub_table_idx);
            return {this, sub_table_idx, impl_it};
        } else {
            return {this, NUM_LEVEL1_SUB_TABLES, level0_sub_table.begin()};
        }
    }

    iterator begin() {
        if (_is_partitioned) {
            size_t sub_table_idx = 0;
            typename Impl::iterator impl_it = begin_of_next_non_empty_sub_table_idx(sub_table_idx);
            return {this, sub_table_idx, impl_it};
        } else {
            return {this, NUM_LEVEL1_SUB_TABLES, level0_sub_table.begin()};
        }
    }

    const_iterator end() const {
        if (_is_partitioned) {
            return {this, MAX_SUB_TABLE, level1_sub_tables[MAX_SUB_TABLE].end()};
        } else {
            return {this, NUM_LEVEL1_SUB_TABLES, level0_sub_table.end()};
        }
    }
    iterator end() {
        if (_is_partitioned) {
            return {this, MAX_SUB_TABLE, level1_sub_tables[MAX_SUB_TABLE].end()};
        } else {
            return {this, NUM_LEVEL1_SUB_TABLES, level0_sub_table.end()};
        }
    }

    /// Insert a value. In the case of any more complex values, it is better to use the `emplace` function.
    std::pair<LookupResult, bool> ALWAYS_INLINE insert(const value_type& x) {
        size_t hash_value = hash(cell_type::get_key(x));

        std::pair<LookupResult, bool> res;
        emplace(cell_type::get_key(x), res.first, res.second, hash_value);

        if (res.second) insert_set_mapped(lookup_result_get_mapped(res.first), x);

        return res;
    }

    void expanse_for_add_elem(size_t num_elem) {
        if (_is_partitioned) {
            size_t num_elem_per_sub_table =
                    (num_elem + NUM_LEVEL1_SUB_TABLES - 1) / NUM_LEVEL1_SUB_TABLES;
            for (size_t i = 0; i < NUM_LEVEL1_SUB_TABLES; ++i) {
                level1_sub_tables[i].expanse_for_add_elem(num_elem_per_sub_table);
            }
        } else {
            level0_sub_table.expanse_for_add_elem(num_elem);
            if (UNLIKELY(level0_sub_table.need_partition())) {
                convert_to_partitioned();
            }
        }
    }

    template <bool READ>
    void ALWAYS_INLINE prefetch(const Key& key, size_t hash_value) {
        if (_is_partitioned) {
            const auto sub_table_idx = get_sub_table_from_hash(hash_value);
            level1_sub_tables[sub_table_idx].template prefetch<READ>(hash_value);
        } else {
            level0_sub_table.template prefetch<READ>(hash_value);
        }
    }

    /** Insert the key,
      * return an iterator to a position that can be used for `placement new` of value,
      * as well as the flag - whether a new key was inserted.
      *
      * You have to make `placement new` values if you inserted a new key,
      * since when destroying a hash table, the destructor will be invoked for it!
      *
      * Example usage:
      *
      * Map::iterator it;
      * bool inserted;
      * map.emplace(key, it, inserted);
      * if (inserted)
      *     new(&it->second) Mapped(value);
      */
    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder&& key_holder, LookupResult& it, bool& inserted) {
        size_t hash_value = hash(key_holder);
        emplace(key_holder, it, inserted, hash_value);
    }

    /// Same, but with a precalculated values of hash function.
    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder&& key_holder, LookupResult& it, bool& inserted,
                               size_t hash_value) {
        if (_is_partitioned) {
            size_t sub_table_idx = get_sub_table_from_hash(hash_value);
            level1_sub_tables[sub_table_idx].emplace(key_holder, it, inserted, hash_value);
        } else {
            level0_sub_table.emplace(key_holder, it, inserted, hash_value);
            if (UNLIKELY(level0_sub_table.need_partition())) {
                convert_to_partitioned();

                // The hash table was converted to partitioned, so we have to re-find the key.
                size_t sub_table_id = get_sub_table_from_hash(hash_value);
                it = level1_sub_tables[sub_table_id].find(key_holder, hash_value);
            }
        }
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder&& key_holder, LookupResult& it, size_t hash_value,
                               bool& inserted) {
        emplace(key_holder, it, inserted, hash_value);
    }

    template <typename KeyHolder, typename Func>
    void ALWAYS_INLINE lazy_emplace(KeyHolder&& key_holder, LookupResult& it, Func&& f) {
        size_t hash_value = hash(key_holder);
        lazy_emplace(key_holder, it, hash_value, std::forward<Func>(f));
    }

    template <typename KeyHolder, typename Func>
    void ALWAYS_INLINE lazy_emplace(KeyHolder&& key_holder, LookupResult& it, size_t hash_value,
                                    Func&& f) {
        if (_is_partitioned) {
            size_t sub_table_idx = get_sub_table_from_hash(hash_value);
            level1_sub_tables[sub_table_idx].lazy_emplace(key_holder, it, hash_value,
                                                          std::forward<Func>(f));
        } else {
            level0_sub_table.lazy_emplace(key_holder, it, hash_value, std::forward<Func>(f));
            if (UNLIKELY(level0_sub_table.need_partition())) {
                convert_to_partitioned();

                // The hash table was converted to partitioned, so we have to re-find the key.
                size_t sub_table_id = get_sub_table_from_hash(hash_value);
                it = level1_sub_tables[sub_table_id].find(key_holder, hash_value);
            }
        }
    }

    LookupResult ALWAYS_INLINE find(Key x, size_t hash_value) {
        if (_is_partitioned) {
            size_t sub_table_idx = get_sub_table_from_hash(hash_value);
            return level1_sub_tables[sub_table_idx].find(x, hash_value);
        } else {
            return level0_sub_table.find(x, hash_value);
        }
    }

    ConstLookupResult ALWAYS_INLINE find(Key x, size_t hash_value) const {
        return const_cast<std::decay_t<decltype(*this)>*>(this)->find(x, hash_value);
    }

    LookupResult ALWAYS_INLINE find(Key x) { return find(x, hash(x)); }

    ConstLookupResult ALWAYS_INLINE find(Key x) const { return find(x, hash(x)); }

    size_t size() const {
        if (_is_partitioned) {
            size_t res = 0;
            for (size_t i = 0; i < NUM_LEVEL1_SUB_TABLES; ++i) {
                res += level1_sub_tables[i].size();
            }
            return res;
        } else {
            return level0_sub_table.size();
        }
    }

    std::vector<size_t> sizes() const {
        std::vector<size_t> sizes;
        if (_is_partitioned) {
            for (size_t i = 0; i < NUM_LEVEL1_SUB_TABLES; ++i) {
                sizes.push_back(level1_sub_tables[i].size());
            }
        } else {
            sizes.push_back(level0_sub_table.size());
        }
        return sizes;
    }

    bool empty() const {
        if (_is_partitioned) {
            for (size_t i = 0; i < NUM_LEVEL1_SUB_TABLES; ++i)
                if (!level1_sub_tables[i].empty()) return false;
            return true;
        } else {
            return level0_sub_table.empty();
        }
    }

    bool add_elem_size_overflow(size_t row) const {
        return !_is_partitioned && level0_sub_table.add_elem_size_overflow(row);
    }

private:
    void convert_to_partitioned() {
        SCOPED_RAW_TIMER(&_convert_timer_ns);

        DCHECK(!_is_partitioned);
        _is_partitioned = true;

        auto bucket_count = level0_sub_table.get_buffer_size_in_cells();
        for (size_t i = 0; i < NUM_LEVEL1_SUB_TABLES; ++i) {
            level1_sub_tables[i] = std::move(Impl(bucket_count / NUM_LEVEL1_SUB_TABLES));
        }

        auto it = level0_sub_table.begin();

        if constexpr (HashTableTraits<Impl>::is_phmap) {
            for (; it != level0_sub_table.end(); ++it) {
                size_t hash_value = level0_sub_table.hash(it.get_first());
                size_t sub_table_idx = get_sub_table_from_hash(hash_value);
                level1_sub_tables[sub_table_idx].insert(it.get_first(), hash_value,
                                                        it.get_second());
            }
        } else {
            /// It is assumed that the zero key (stored separately) is first in iteration order.
            if (it != level0_sub_table.end() && it.get_ptr()->is_zero(level0_sub_table)) {
                insert(it->get_value());
                ++it;
            }

            for (; it != level0_sub_table.end(); ++it) {
                const auto* cell = it.get_ptr();
                size_t hash_value = cell->get_hash(level0_sub_table);
                size_t sub_table_idx = get_sub_table_from_hash(hash_value);
                level1_sub_tables[sub_table_idx].insert_unique_non_zero(cell, hash_value);
            }
        }

        level0_sub_table.clear_and_shrink();
    }

    /// NOTE Bad for hash tables with more than 2^32 cells.
    static size_t get_sub_table_from_hash(size_t hash_value) {
        return (hash_value >> (32 - BITS_FOR_SUB_TABLE)) & MAX_SUB_TABLE;
    }
};
