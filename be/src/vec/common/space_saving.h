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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/SpaceSaving.h.h
// and modified by Doris

#pragma once

#include <boost/range/adaptor/reversed.hpp>

#include "vec/common/arena_with_free_lists.h"
#include "vec/common/hash_table/hash_map.h"
#include "vec/common/string_buffer.hpp"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

template <typename TKey>
struct SpaceSavingArena {
    SpaceSavingArena() = default;
    TKey emplace(const TKey& key) { return key; }
    void free(const TKey& /*key*/) {}
};

template <>
struct SpaceSavingArena<StringRef> {
    StringRef emplace(StringRef key) {
        if (!key.data) {
            return key;
        }

        return copy_string_in_arena(arena, key);
    }

    void free(StringRef key) {
        if (key.data) {
            arena.free(const_cast<char*>(key.data), key.size);
        }
    }

    template <typename Arena>
    inline StringRef copy_string_in_arena(Arena& arena, StringRef value) {
        size_t value_size = value.size;
        char* place_for_key = arena.alloc(value_size);
        memcpy(reinterpret_cast<void*>(place_for_key), reinterpret_cast<const void*>(value.data),
               value_size);
        StringRef result {place_for_key, value_size};

        return result;
    }

private:
    ArenaWithFreeLists arena;
};

template <typename TKey, typename Hash = DefaultHash<TKey>>
class SpaceSaving {
private:
    // This function calculates the next size for the alpha map based on the given value `x`.
    // It uses `alpha_map_elements_per_counter` to estimate the required number of elements.
    constexpr uint64_t next_alpha_size(uint64_t x) {
        constexpr uint64_t alpha_map_elements_per_counter = 6;
        return 1ULL << (sizeof(uint64_t) * 8 -
                        std::countl_zero(x * alpha_map_elements_per_counter));
    }

public:
    using Self = SpaceSaving;

    struct Counter {
        Counter() = default;

        explicit Counter(const TKey& k, uint64_t c = 0, uint64_t e = 0, size_t h = 0)
                : key(k), hash(h), count(c), error(e) {}

        void write(BufferWritable& wb) const {
            write_binary(key, wb);
            write_var_uint(count, wb);
            write_var_uint(error, wb);
        }

        void read(BufferReadable& rb) {
            read_binary(key, rb);
            read_var_uint(count, rb);
            read_var_uint(error, rb);
        }

        bool operator>(const Counter& b) const {
            return (count > b.count) || (count == b.count && error < b.error);
        }

        TKey key;
        size_t slot = 0;
        size_t hash = 0;
        uint64_t count = 0;
        uint64_t error = 0;
    };

    explicit SpaceSaving(size_t c = 10) : alpha_map(next_alpha_size(c)), m_capacity(c) {}

    ~SpaceSaving() { destroy_elements(); }

    size_t size() const { return counter_list.size(); }

    size_t capacity() const { return m_capacity; }

    void clear() { return destroy_elements(); }

    void resize(size_t new_capacity) {
        counter_list.reserve(new_capacity);
        alpha_map.resize(next_alpha_size(new_capacity));
        m_capacity = new_capacity;
    }

    // Inserts a new element or updates the count of an existing element.
    // If the element exists, the count and error are increased.
    // If the element doesn't exist and the capacity is not full, it inserts the new element.
    // If the capacity is full, it replaces the element with the smallest count and inserts the new one.
    void insert(const TKey& key, uint64_t increment = 1, uint64_t error = 0) {
        auto hash = counter_map.hash(key);

        if (auto* counter = find_counter(key, hash); counter) {
            counter->count += increment;
            counter->error += error;
            percolate(counter);
            return;
        }

        if (UNLIKELY(size() < capacity())) {
            push(std::make_unique<Counter>(arena.emplace(key), increment, error, hash));
            return;
        }

        auto& min = counter_list.back();
        if (increment > min->count) {
            destroy_last_element();
            push(std::make_unique<Counter>(arena.emplace(key), increment, error, hash));
            return;
        }

        const size_t alpha_mask = alpha_map.size() - 1;
        auto& alpha = alpha_map[hash & alpha_mask];
        if (alpha + increment < min->count) {
            alpha += increment;
            return;
        }

        alpha_map[min->hash & alpha_mask] = min->count;
        destroy_last_element();

        push(std::make_unique<Counter>(arena.emplace(key), alpha + increment, alpha + error, hash));
    }

    // Merges another `SpaceSaving` object into the current one. Updates counts and errors of elements.
    // If the other object is full, it adds its elements to the current list and maintains sorting.
    void merge(const Self& rhs) {
        if (!rhs.size()) {
            return;
        }

        uint64_t m1 = 0;
        uint64_t m2 = 0;

        if (size() == capacity()) {
            m1 = counter_list.back()->count;
        }

        if (rhs.size() == rhs.capacity()) {
            m2 = rhs.counter_list.back()->count;
        }

        if (m2 > 0) {
            for (auto& counter : counter_list) {
                counter->count += m2;
                counter->error += m2;
            }
        }

        for (auto& counter : boost::adaptors::reverse(rhs.counter_list)) {
            size_t hash = counter_map.hash(counter->key);
            if (auto* current = find_counter(counter->key, hash)) {
                current->count += (counter->count - m2);
                current->error += (counter->error - m2);
            } else {
                counter_list.push_back(std::make_unique<Counter>(arena.emplace(counter->key),
                                                                 counter->count + m1,
                                                                 counter->error + m1, hash));
            }
        }

        std::sort(counter_list.begin(), counter_list.end(),
                  [](const auto& l, const auto& r) { return *l > *r; });

        if (counter_list.size() > m_capacity) {
            for (size_t i = m_capacity; i < counter_list.size(); ++i) {
                arena.free(counter_list[i]->key);
            }
            counter_list.resize(m_capacity);
        }

        for (size_t i = 0; i < counter_list.size(); ++i) {
            counter_list[i]->slot = i;
        }
        rebuild_counter_map();
    }

    // Retrieves the top-k counters, sorted by their count and error values.
    std::vector<Counter> top_k(size_t k) const {
        std::vector<Counter> res;
        for (auto& counter : counter_list) {
            res.push_back(*counter);
            if (res.size() == k) {
                break;
            }
        }
        return res;
    }

    void write(BufferWritable& wb) const {
        write_var_uint(size(), wb);
        for (auto& counter : counter_list) {
            counter->write(wb);
        }

        write_var_uint(alpha_map.size(), wb);
        for (auto alpha : alpha_map) {
            write_var_uint(alpha, wb);
        }
    }

    void read(BufferReadable& rb) {
        destroy_elements();
        uint64_t count = 0;
        read_var_uint(count, rb);

        for (UInt64 i = 0; i < count; ++i) {
            std::unique_ptr counter = std::make_unique<Counter>();
            counter->read(rb);
            counter->hash = counter_map.hash(counter->key);
            push(std::move(counter));
        }

        read_alpha_map(rb);
    }

    // Reads the alpha map data from the provided readable buffer.
    void read_alpha_map(BufferReadable& rb) {
        uint64_t alpha_size = 0;
        read_var_uint(alpha_size, rb);
        for (size_t i = 0; i < alpha_size; ++i) {
            uint64_t alpha = 0;
            read_var_uint(alpha, rb);
            alpha_map.push_back(alpha);
        }
    }

protected:
    void push(std::unique_ptr<Counter> counter) {
        counter->slot = counter_list.size();
        auto* ptr = counter.get();
        counter_list.push_back(std::move(counter));
        counter_map[ptr->key] = ptr;
        percolate(ptr);
    }

    void percolate(Counter* counter) {
        while (counter->slot > 0) {
            auto& next = counter_list[counter->slot - 1];
            if (*counter > *next) {
                std::swap(next->slot, counter->slot);
                std::swap(counter_list[next->slot], counter_list[counter->slot]);
            } else {
                break;
            }
        }
    }

private:
    void destroy_elements() {
        for (auto& counter : counter_list) {
            arena.free(counter->key);
        }

        counter_map.clear();
        counter_list.clear();
        alpha_map.clear();
    }

    void destroy_last_element() {
        auto& last_element = counter_list.back();
        counter_map.erase(last_element->key);
        arena.free(last_element->key);
        counter_list.pop_back();

        ++removed_keys;
        if (removed_keys * 2 > counter_map.size()) {
            rebuild_counter_map();
        }
    }

    Counter* find_counter(const TKey& key, size_t hash) {
        auto it = counter_map.find(key, hash);
        if (it == counter_map.end()) {
            return nullptr;
        }

        return it->second;
    }

    void rebuild_counter_map() {
        removed_keys = 0;
        counter_map.clear();
        for (auto& counter : counter_list) {
            counter_map[counter->key] = counter.get();
        }
    }

    using CounterMap = flat_hash_map<TKey, Counter*, Hash>;

    CounterMap counter_map;
    std::vector<std::unique_ptr<Counter>> counter_list;
    std::vector<uint64_t> alpha_map;
    SpaceSavingArena<TKey> arena;
    size_t m_capacity = 0;
    size_t removed_keys = 0;
};

} // namespace doris::vectorized