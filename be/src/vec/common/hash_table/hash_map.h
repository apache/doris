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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/HashTable/HashMap.h
// and modified by Doris

#pragma once

#include <gen_cpp/PlanNodes_types.h>

#include "common/compiler_util.h"
#include "vec/columns/column_filter_helper.h"
#include "vec/common/hash_table/hash.h"
#include "vec/common/hash_table/hash_table.h"
#include "vec/common/hash_table/hash_table_allocator.h"

/** NOTE HashMap could only be used for memmoveable (position independent) types.
  * Example: std::string is not position independent in libstdc++ with C++11 ABI or in libc++.
  * Also, key in hash table must be of type, that zero bytes is compared equals to zero key.
  */

struct NoInitTag {};

/// A pair that does not initialize the elements, if not needed.
template <typename First, typename Second>
struct PairNoInit {
    First first;
    Second second;

    PairNoInit() {}

    template <typename First_>
    PairNoInit(First_&& first_, NoInitTag) : first(std::forward<First_>(first_)) {}

    template <typename First_, typename Second_>
    PairNoInit(First_&& first_, Second_&& second_)
            : first(std::forward<First_>(first_)), second(std::forward<Second_>(second_)) {}
};

template <typename Key, typename TMapped, typename Hash, typename TState = HashTableNoState>
struct HashMapCell {
    using Mapped = TMapped;
    using State = TState;

    using value_type = PairNoInit<Key, Mapped>;
    using mapped_type = Mapped;
    using key_type = Key;

    value_type value;

    HashMapCell() = default;
    HashMapCell(const Key& key_, const State&) : value(key_, NoInitTag()) {}
    HashMapCell(const Key& key_, const Mapped& mapped_) : value(key_, mapped_) {}
    HashMapCell(const value_type& value_, const State&) : value(value_) {}

    const Key& get_first() const { return value.first; }
    Mapped& get_second() { return value.second; }
    const Mapped& get_second() const { return value.second; }

    const value_type& get_value() const { return value; }

    static const Key& get_key(const value_type& value) { return value.first; }
    Mapped& get_mapped() { return value.second; }
    const Mapped& get_mapped() const { return value.second; }

    bool key_equals(const Key& key_) const { return value.first == key_; }
    bool key_equals(const Key& key_, size_t /*hash_*/) const { return value.first == key_; }
    bool key_equals(const Key& key_, size_t /*hash_*/, const State& /*state*/) const {
        return value.first == key_;
    }

    void set_hash(size_t /*hash_value*/) {}
    size_t get_hash(const Hash& hash) const { return hash(value.first); }

    bool is_zero(const State& state) const { return is_zero(value.first, state); }
    static bool is_zero(const Key& key, const State& /*state*/) { return ZeroTraits::check(key); }

    /// Set the key value to zero.
    void set_zero() { ZeroTraits::set(value.first); }

    /// Do I need to store the zero key separately (that is, can a zero key be inserted into the hash table).
    static constexpr bool need_zero_value_storage = true;

    void set_mapped(const value_type& value_) { value.second = value_.second; }
};

template <typename Key, typename Mapped, typename Hash, typename State>
ALWAYS_INLINE inline auto lookup_result_get_key(HashMapCell<Key, Mapped, Hash, State>* cell) {
    return &cell->get_first();
}

template <typename Key, typename Mapped, typename Hash, typename State>
ALWAYS_INLINE inline auto lookup_result_get_mapped(HashMapCell<Key, Mapped, Hash, State>* cell) {
    return &cell->get_second();
}

template <typename Key, typename TMapped, typename Hash, typename TState = HashTableNoState>
struct HashMapCellWithSavedHash : public HashMapCell<Key, TMapped, Hash, TState> {
    using Base = HashMapCell<Key, TMapped, Hash, TState>;

    size_t saved_hash;

    using Base::Base;

    bool key_equals(const Key& key_) const { return this->value.first == key_; }
    bool key_equals(const Key& key_, size_t hash_) const {
        return saved_hash == hash_ && this->value.first == key_;
    }
    bool key_equals(const Key& key_, size_t hash_, const typename Base::State&) const {
        return key_equals(key_, hash_);
    }

    void set_hash(size_t hash_value) { saved_hash = hash_value; }
    size_t get_hash(const Hash& /*hash_function*/) const { return saved_hash; }
};

template <typename Key, typename Mapped, typename Hash, typename State>
ALWAYS_INLINE inline auto lookup_result_get_key(
        HashMapCellWithSavedHash<Key, Mapped, Hash, State>* cell) {
    return &cell->get_first();
}

template <typename Key, typename Mapped, typename Hash, typename State>
ALWAYS_INLINE inline auto lookup_result_get_mapped(
        HashMapCellWithSavedHash<Key, Mapped, Hash, State>* cell) {
    return &cell->get_second();
}

template <typename Key, typename Cell, typename Hash = DefaultHash<Key>,
          typename Grower = HashTableGrower<>, typename Allocator = HashTableAllocator>
class HashMapTable : public HashTable<Key, Cell, Hash, Grower, Allocator> {
public:
    using Self = HashMapTable;
    using Base = HashTable<Key, Cell, Hash, Grower, Allocator>;

    using key_type = Key;
    using value_type = typename Cell::value_type;
    using mapped_type = typename Cell::Mapped;

    using LookupResult = typename Base::LookupResult;

    using HashTable<Key, Cell, Hash, Grower, Allocator>::HashTable;

    /// Call func(Mapped &) for each hash map element.
    template <typename Func>
    void for_each_mapped(Func&& func) {
        for (auto& v : *this) func(v.get_second());
    }

    mapped_type& ALWAYS_INLINE operator[](Key x) {
        typename HashMapTable::LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted);

        /** It may seem that initialization is not necessary for POD-types (or __has_trivial_constructor),
          *  since the hash table memory is initially initialized with zeros.
          * But, in fact, an empty cell may not be initialized with zeros in the following cases:
          * - ZeroValueStorage (it only zeros the key);
          * - after resizing and moving a part of the cells to the new half of the hash table, the old cells also have only the key to zero.
          *
          * On performance, there is almost always no difference, due to the fact that it->second is usually assigned immediately
          *  after calling `operator[]`, and since `operator[]` is inlined, the compiler removes unnecessary initialization.
          *
          * Sometimes due to initialization, the performance even grows. This occurs in code like `++map[key]`.
          * When we do the initialization, for new cells, it's enough to make `store 1` right away.
          * And if we did not initialize, then even though there was zero in the cell,
          *  the compiler can not guess about this, and generates the `load`, `increment`, `store` code.
          */
        if (inserted) new (lookup_result_get_mapped(it)) mapped_type();

        return *lookup_result_get_mapped(it);
    }

    template <typename MappedType>
    char* get_null_key_data() {
        return nullptr;
    }
    bool has_null_key_data() const { return false; }
};

template <typename Key, typename Cell, typename Hash = DefaultHash<Key>,
          typename Grower = HashTableGrower<>, typename Allocator = HashTableAllocator>
class JoinHashMapTable : public HashMapTable<Key, Cell, Hash, Grower, Allocator> {
public:
    using Self = JoinHashMapTable;
    using Base = HashMapTable<Key, Cell, Hash, Grower, Allocator>;

    using key_type = Key;
    using value_type = typename Cell::value_type;
    using mapped_type = typename Cell::Mapped;

    using LookupResult = typename Base::LookupResult;

    static uint32_t calc_bucket_size(size_t num_elem) {
        size_t expect_bucket_size = num_elem + (num_elem - 1) / 7;
        return phmap::priv::NormalizeCapacity(expect_bucket_size) + 1;
    }

    size_t get_byte_size() const {
        auto cal_vector_mem = [](const auto& vec) { return vec.capacity() * sizeof(vec[0]); };
        return cal_vector_mem(visited) + cal_vector_mem(first) + cal_vector_mem(next);
    }

    template <int JoinOpType>
    void prepare_build(size_t num_elem, int batch_size, bool has_null_key) {
        _has_null_key = has_null_key;

        // the first row in build side is not really from build side table
        _empty_build_side = num_elem <= 1;
        max_batch_size = batch_size;
        bucket_size = calc_bucket_size(num_elem + 1);
        first.resize(bucket_size + 1);
        next.resize(num_elem);

        if constexpr (JoinOpType == doris::TJoinOp::FULL_OUTER_JOIN ||
                      JoinOpType == doris::TJoinOp::RIGHT_OUTER_JOIN ||
                      JoinOpType == doris::TJoinOp::RIGHT_ANTI_JOIN ||
                      JoinOpType == doris::TJoinOp::RIGHT_SEMI_JOIN) {
            visited.resize(num_elem);
        }
    }

    uint32_t get_bucket_size() const { return bucket_size; }

    size_t size() const { return Base::size() == 0 ? next.size() : Base::size(); }

    std::vector<uint8_t>& get_visited() { return visited; }

    void build(const Key* __restrict keys, const uint32_t* __restrict bucket_nums,
               size_t num_elem) {
        build_keys = keys;
        for (size_t i = 1; i < num_elem; i++) {
            uint32_t bucket_num = bucket_nums[i];
            next[i] = first[bucket_num];
            first[bucket_num] = i;
        }
        first[bucket_size] = 0; // index = bucket_num means null
    }

    template <int JoinOpType, bool with_other_conjuncts, bool is_mark_join, bool need_judge_null>
    auto find_batch(const Key* __restrict keys, const uint32_t* __restrict build_idx_map,
                    int probe_idx, uint32_t build_idx, int probe_rows,
                    uint32_t* __restrict probe_idxs, bool& probe_visited,
                    uint32_t* __restrict build_idxs,
                    doris::vectorized::ColumnFilterHelper* mark_column) {
        if constexpr (JoinOpType == doris::TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
            if (_empty_build_side) {
                return _process_null_aware_left_anti_join_for_empty_build_side<
                        JoinOpType, with_other_conjuncts, is_mark_join>(
                        probe_idx, probe_rows, probe_idxs, build_idxs, mark_column);
            }
        }

        if constexpr (with_other_conjuncts) {
            return _find_batch_conjunct<JoinOpType>(keys, build_idx_map, probe_idx, build_idx,
                                                    probe_rows, probe_idxs, build_idxs);
        }

        if constexpr (is_mark_join) {
            return _find_batch_mark<JoinOpType>(keys, build_idx_map, probe_idx, probe_rows,
                                                probe_idxs, build_idxs, mark_column);
        }

        if constexpr (JoinOpType == doris::TJoinOp::INNER_JOIN ||
                      JoinOpType == doris::TJoinOp::FULL_OUTER_JOIN ||
                      JoinOpType == doris::TJoinOp::LEFT_OUTER_JOIN ||
                      JoinOpType == doris::TJoinOp::RIGHT_OUTER_JOIN) {
            return _find_batch_inner_outer_join<JoinOpType>(keys, build_idx_map, probe_idx,
                                                            build_idx, probe_rows, probe_idxs,
                                                            probe_visited, build_idxs);
        }
        if constexpr (JoinOpType == doris::TJoinOp::LEFT_ANTI_JOIN ||
                      JoinOpType == doris::TJoinOp::LEFT_SEMI_JOIN ||
                      JoinOpType == doris::TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
            return _find_batch_left_semi_anti<JoinOpType, need_judge_null>(
                    keys, build_idx_map, probe_idx, probe_rows, probe_idxs);
        }
        if constexpr (JoinOpType == doris::TJoinOp::RIGHT_ANTI_JOIN ||
                      JoinOpType == doris::TJoinOp::RIGHT_SEMI_JOIN) {
            return _find_batch_right_semi_anti(keys, build_idx_map, probe_idx, probe_rows);
        }
        return std::tuple {0, 0U, 0};
    }

    template <int JoinOpType>
    bool iterate_map(std::vector<uint32_t>& build_idxs) const {
        const auto batch_size = max_batch_size;
        const auto elem_num = visited.size();
        int count = 0;
        build_idxs.resize(batch_size);

        while (count < batch_size && iter_idx < elem_num) {
            const auto matched = visited[iter_idx];
            build_idxs[count] = iter_idx;
            if constexpr (JoinOpType != doris::TJoinOp::RIGHT_SEMI_JOIN) {
                count += !matched;
            } else {
                count += matched;
            }
            iter_idx++;
        }

        build_idxs.resize(count);
        return iter_idx >= elem_num;
    }

    bool has_null_key() { return _has_null_key; }

    void pre_build_idxs(std::vector<uint32>& bucksets, const uint8_t* null_map) {
        if (null_map) {
            first[bucket_size] = bucket_size; // distinguish between not matched and null
        }

        for (uint32_t i = 0; i < bucksets.size(); i++) {
            bucksets[i] = first[bucksets[i]];
        }
    }

private:
    // only LEFT_ANTI_JOIN/LEFT_SEMI_JOIN/NULL_AWARE_LEFT_ANTI_JOIN/CROSS_JOIN support mark join
    template <int JoinOpType>
    auto _find_batch_mark(const Key* __restrict keys, const uint32_t* __restrict build_idx_map,
                          int probe_idx, int probe_rows, uint32_t* __restrict probe_idxs,
                          uint32_t* __restrict build_idxs,
                          doris::vectorized::ColumnFilterHelper* mark_column) {
        auto matched_cnt = 0;
        const auto batch_size = max_batch_size;

        while (probe_idx < probe_rows && matched_cnt < batch_size) {
            auto build_idx = build_idx_map[probe_idx] == bucket_size ? 0 : build_idx_map[probe_idx];

            while (build_idx && keys[probe_idx] != build_keys[build_idx]) {
                build_idx = next[build_idx];
            }

            if (build_idx_map[probe_idx] == bucket_size) {
                // mark result as null when probe row is null
                mark_column->insert_null();
            } else {
                bool matched = JoinOpType == doris::TJoinOp::LEFT_SEMI_JOIN ? build_idx != 0
                                                                            : build_idx == 0;
                if (!matched && _has_null_key) {
                    mark_column->insert_null();
                } else {
                    mark_column->insert_value(matched);
                }
            }

            probe_idxs[matched_cnt] = probe_idx++;
            build_idxs[matched_cnt] = build_idx;
            matched_cnt++;
        }
        return std::tuple {probe_idx, 0U, matched_cnt};
    }

    template <int JoinOpType, bool with_other_conjuncts, bool is_mark_join>
    auto _process_null_aware_left_anti_join_for_empty_build_side(
            int probe_idx, int probe_rows, uint32_t* __restrict probe_idxs,
            uint32_t* __restrict build_idxs, doris::vectorized::ColumnFilterHelper* mark_column) {
        static_assert(JoinOpType == doris::TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN);
        auto matched_cnt = 0;
        const auto batch_size = max_batch_size;

        while (probe_idx < probe_rows && matched_cnt < batch_size) {
            probe_idxs[matched_cnt] = probe_idx++;
            if constexpr (is_mark_join) {
                build_idxs[matched_cnt] = 0;
            }
            ++matched_cnt;
        }

        if constexpr (is_mark_join && !with_other_conjuncts) {
            mark_column->resize_fill(matched_cnt, 1);
        }

        return std::tuple {probe_idx, 0U, matched_cnt};
    }

    auto _find_batch_right_semi_anti(const Key* __restrict keys,
                                     const uint32_t* __restrict build_idx_map, int probe_idx,
                                     int probe_rows) {
        while (probe_idx < probe_rows) {
            auto build_idx = build_idx_map[probe_idx];

            while (build_idx) {
                if (!visited[build_idx] && keys[probe_idx] == build_keys[build_idx]) {
                    visited[build_idx] = 1;
                }
                build_idx = next[build_idx];
            }
            probe_idx++;
        }
        return std::tuple {probe_idx, 0U, 0};
    }

    template <int JoinOpType, bool need_judge_null>
    auto _find_batch_left_semi_anti(const Key* __restrict keys,
                                    const uint32_t* __restrict build_idx_map, int probe_idx,
                                    int probe_rows, uint32_t* __restrict probe_idxs) {
        auto matched_cnt = 0;
        const auto batch_size = max_batch_size;

        while (probe_idx < probe_rows && matched_cnt < batch_size) {
            if constexpr (need_judge_null) {
                if (build_idx_map[probe_idx] == bucket_size) {
                    probe_idx++;
                    continue;
                }
            }

            auto build_idx = build_idx_map[probe_idx];

            while (build_idx && keys[probe_idx] != build_keys[build_idx]) {
                build_idx = next[build_idx];
            }
            bool matched =
                    JoinOpType == doris::TJoinOp::LEFT_SEMI_JOIN ? build_idx != 0 : build_idx == 0;
            probe_idxs[matched_cnt] = probe_idx++;
            matched_cnt += matched;
        }
        return std::tuple {probe_idx, 0U, matched_cnt};
    }

    template <int JoinOpType>
    auto _find_batch_conjunct(const Key* __restrict keys, const uint32_t* __restrict build_idx_map,
                              int probe_idx, uint32_t build_idx, int probe_rows,
                              uint32_t* __restrict probe_idxs, uint32_t* __restrict build_idxs) {
        auto matched_cnt = 0;
        const auto batch_size = max_batch_size;

        auto do_the_probe = [&]() {
            while (build_idx && matched_cnt < batch_size) {
                if constexpr (JoinOpType == doris::TJoinOp::RIGHT_ANTI_JOIN ||
                              JoinOpType == doris::TJoinOp::RIGHT_SEMI_JOIN) {
                    if (!visited[build_idx] && keys[probe_idx] == build_keys[build_idx]) {
                        probe_idxs[matched_cnt] = probe_idx;
                        build_idxs[matched_cnt] = build_idx;
                        matched_cnt++;
                    }
                } else if (keys[probe_idx] == build_keys[build_idx]) {
                    build_idxs[matched_cnt] = build_idx;
                    probe_idxs[matched_cnt] = probe_idx;
                    matched_cnt++;
                }
                build_idx = next[build_idx];
            }

            if constexpr (JoinOpType == doris::TJoinOp::LEFT_OUTER_JOIN ||
                          JoinOpType == doris::TJoinOp::FULL_OUTER_JOIN ||
                          JoinOpType == doris::TJoinOp::LEFT_SEMI_JOIN ||
                          JoinOpType == doris::TJoinOp::LEFT_ANTI_JOIN ||
                          JoinOpType == doris::TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
                // may over batch_size when emplace 0 into build_idxs
                if (!build_idx) {
                    probe_idxs[matched_cnt] = probe_idx;
                    build_idxs[matched_cnt] = 0;
                    matched_cnt++;
                }
            }

            probe_idx++;
        };

        if (build_idx) {
            do_the_probe();
        }

        while (probe_idx < probe_rows && matched_cnt < batch_size) {
            build_idx = build_idx_map[probe_idx];
            do_the_probe();
        }

        probe_idx -= (build_idx != 0);
        return std::tuple {probe_idx, build_idx, matched_cnt};
    }

    template <int JoinOpType>
    auto _find_batch_inner_outer_join(const Key* __restrict keys,
                                      const uint32_t* __restrict build_idx_map, int probe_idx,
                                      uint32_t build_idx, int probe_rows,
                                      uint32_t* __restrict probe_idxs, bool& probe_visited,
                                      uint32_t* __restrict build_idxs) {
        auto matched_cnt = 0;
        const auto batch_size = max_batch_size;

        auto do_the_probe = [&]() {
            while (build_idx && matched_cnt < batch_size) {
                if (keys[probe_idx] == build_keys[build_idx]) {
                    probe_idxs[matched_cnt] = probe_idx;
                    build_idxs[matched_cnt] = build_idx;
                    matched_cnt++;
                    if constexpr (JoinOpType == doris::TJoinOp::RIGHT_OUTER_JOIN ||
                                  JoinOpType == doris::TJoinOp::FULL_OUTER_JOIN) {
                        if (!visited[build_idx]) {
                            visited[build_idx] = 1;
                        }
                    }
                }
                build_idx = next[build_idx];
            }

            if constexpr (JoinOpType == doris::TJoinOp::LEFT_OUTER_JOIN ||
                          JoinOpType == doris::TJoinOp::FULL_OUTER_JOIN) {
                // `(!matched_cnt || probe_idxs[matched_cnt - 1] != probe_idx)` means not match one build side
                probe_visited |= (matched_cnt && probe_idxs[matched_cnt - 1] == probe_idx);
                if (!build_idx) {
                    if (!probe_visited) {
                        probe_idxs[matched_cnt] = probe_idx;
                        build_idxs[matched_cnt] = 0;
                        matched_cnt++;
                    }
                    probe_visited = false;
                }
            }
            probe_idx++;
        };

        if (build_idx) {
            do_the_probe();
        }

        while (probe_idx < probe_rows && matched_cnt < batch_size) {
            build_idx = build_idx_map[probe_idx];
            do_the_probe();
        }

        probe_idx -= (build_idx != 0);
        return std::tuple {probe_idx, build_idx, matched_cnt};
    }

    const Key* __restrict build_keys;
    std::vector<uint8_t> visited;

    uint32_t bucket_size = 1;
    int max_batch_size = 4064;

    std::vector<uint32_t> first = {0};
    std::vector<uint32_t> next = {0};

    // use in iter hash map
    mutable uint32_t iter_idx = 1;
    Cell cell;
    doris::vectorized::Arena* pool;
    bool _has_null_key = false;
    bool _empty_build_side = true;
};

template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>,
          typename Grower = HashTableGrower<>, typename Allocator = HashTableAllocator>
using HashMap = HashMapTable<Key, HashMapCell<Key, Mapped, Hash>, Hash, Grower, Allocator>;

template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>>
using JoinFixedHashMap = JoinHashMapTable<Key, HashMapCell<Key, Mapped, Hash>, Hash>;

template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>,
          typename Grower = HashTableGrower<>, typename Allocator = HashTableAllocator>
using HashMapWithSavedHash =
        HashMapTable<Key, HashMapCellWithSavedHash<Key, Mapped, Hash>, Hash, Grower, Allocator>;

template <typename Key, typename Mapped, typename Hash, size_t initial_size_degree>
using HashMapWithStackMemory = HashMapTable<
        Key, HashMapCellWithSavedHash<Key, Mapped, Hash>, Hash,
        HashTableGrower<initial_size_degree>,
        HashTableAllocatorWithStackMemory<(1ULL << initial_size_degree) *
                                          sizeof(HashMapCellWithSavedHash<Key, Mapped, Hash>)>>;
