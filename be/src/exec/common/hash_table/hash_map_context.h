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

#include <algorithm>
#include <cstdint>
#include <type_traits>
#include <utility>

#include "common/compiler_util.h"
#include "core/arena.h"
#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/custom_allocator.h"
#include "core/string_ref.h"
#include "core/types.h"
#include "exec/common/columns_hashing.h"
#include "exec/common/hash_table/string_hash_map.h"
#include "exec/common/template_helpers.hpp"
#include "util/simd/bits.h"

namespace doris {
constexpr auto BITSIZE = 8;

template <typename Base>
struct DataWithNullKey;

template <typename HashMap>
struct MethodBaseInner {
    using Key = typename HashMap::key_type;
    using Mapped = typename HashMap::mapped_type;
    using Value = typename HashMap::value_type;
    using HashMapType = HashMap;

    std::shared_ptr<HashMap> hash_table = nullptr;
    Key* keys = nullptr;
    Arena arena;
    DorisVector<size_t> hash_values;

    /// Reusable buffer for source-side output iteration to avoid per-batch
    /// heap allocation of std::vector<Key>. Callers use resize() + direct
    /// element assignment, so the capacity is retained across batches.
    std::vector<Key> output_keys;

    // use in join case
    DorisVector<uint32_t> bucket_nums;

    MethodBaseInner() { hash_table.reset(new HashMap()); }
    virtual ~MethodBaseInner() = default;

    virtual void init_serialized_keys(const ColumnRawPtrs& key_columns, uint32_t num_rows,
                                      const uint8_t* null_map = nullptr, bool is_join = false,
                                      bool is_build = false, uint32_t bucket_size = 0) = 0;

    [[nodiscard]] virtual size_t estimated_size(const ColumnRawPtrs& key_columns, uint32_t num_rows,
                                                bool is_join = false, bool is_build = false,
                                                uint32_t bucket_size = 0) = 0;

    virtual size_t serialized_keys_size(bool is_build) const { return 0; }

    void init_join_bucket_num(uint32_t num_rows, uint32_t bucket_size, const uint8_t* null_map) {
        bucket_nums.resize(num_rows);

        if (null_map == nullptr) {
            init_join_bucket_num(num_rows, bucket_size);
            return;
        }
        for (uint32_t k = 0; k < num_rows; ++k) {
            bucket_nums[k] =
                    null_map[k] ? bucket_size : hash_table->hash(keys[k]) & (bucket_size - 1);
        }
    }

    void init_join_bucket_num(uint32_t num_rows, uint32_t bucket_size) {
        for (uint32_t k = 0; k < num_rows; ++k) {
            bucket_nums[k] = hash_table->hash(keys[k]) & (bucket_size - 1);
        }
    }

    void init_hash_values(uint32_t num_rows, const uint8_t* null_map) {
        if (null_map == nullptr) {
            init_hash_values(num_rows);
            return;
        }
        hash_values.resize(num_rows);
        for (size_t k = 0; k < num_rows; ++k) {
            if (null_map[k]) {
                continue;
            }

            hash_values[k] = hash_table->hash(keys[k]);
        }
    }

    void init_hash_values(uint32_t num_rows) {
        hash_values.resize(num_rows);
        for (size_t k = 0; k < num_rows; ++k) {
            hash_values[k] = hash_table->hash(keys[k]);
        }
    }

    template <bool read>
    void prefetch(size_t i) {
        if (LIKELY(i + HASH_MAP_PREFETCH_DIST < hash_values.size())) {
            hash_table->template prefetch<read>(keys[i + HASH_MAP_PREFETCH_DIST],
                                                hash_values[i + HASH_MAP_PREFETCH_DIST]);
        }
    }

    template <typename State>
    auto find(State& state, size_t i) {
        prefetch<true>(i);
        return state.find_key_with_hash(*hash_table, i, keys[i], hash_values[i]);
    }

    template <typename State, typename F, typename FF>
    auto lazy_emplace(State& state, size_t i, F&& creator, FF&& creator_for_null_key) {
        prefetch<false>(i);
        return state.lazy_emplace_key(*hash_table, i, keys[i], hash_values[i], creator,
                                      creator_for_null_key);
    }

    static constexpr bool is_string_hash_map() {
        return std::is_same_v<StringHashMap<Mapped>, HashMap> ||
               std::is_same_v<DataWithNullKey<StringHashMap<Mapped>>, HashMap>;
    }

    template <typename Key, typename Origin>
    static void try_presis_key(Key& key, Origin& origin, Arena& arena) {
        if constexpr (std::is_same_v<Key, StringRef>) {
            key.data = arena.insert(key.data, key.size);
        }
    }

    template <typename Key, typename Origin>
    static void try_presis_key_and_origin(Key& key, Origin& origin, Arena& arena) {
        if constexpr (std::is_same_v<Origin, StringRef>) {
            origin.data = arena.insert(origin.data, origin.size);
            if constexpr (!is_string_hash_map()) {
                key = origin;
            }
        }
    }

    virtual void insert_keys_into_columns(std::vector<Key>& keys, MutableColumns& key_columns,
                                          uint32_t num_rows) = 0;

    virtual uint32_t direct_mapping_range() { return 0; }
};

template <typename T>
concept IteratoredMap = requires(T* map) { typename T::iterator; };

template <typename HashMap>
struct MethodBase : public MethodBaseInner<HashMap> {
    using Iterator = void*;
    void init_iterator() {}
};

template <IteratoredMap HashMap>
struct MethodBase<HashMap> : public MethodBaseInner<HashMap> {
    using Iterator = typename HashMap::iterator;
    using Base = MethodBaseInner<HashMap>;
    Iterator begin;
    Iterator end;
    bool inited_iterator = false;
    void init_iterator() {
        if (!inited_iterator) {
            inited_iterator = true;
            begin = Base::hash_table->begin();
            end = Base::hash_table->end();
        }
    }
};

template <typename TData>
struct MethodSerialized : public MethodBase<TData> {
    using Base = MethodBase<TData>;
    using Base::init_iterator;
    using State = ColumnsHashing::HashMethodSerialized<typename Base::Value, typename Base::Mapped>;
    using Base::try_presis_key;
    // need keep until the hash probe end.
    DorisVector<StringRef> build_stored_keys;
    Arena build_arena;
    // refresh each time probe
    DorisVector<StringRef> stored_keys;

    StringRef serialize_keys_to_pool_contiguous(size_t i, size_t keys_size,
                                                const ColumnRawPtrs& key_columns, Arena& pool) {
        const char* begin = nullptr;

        size_t sum_size = 0;
        for (size_t j = 0; j < keys_size; ++j) {
            sum_size += key_columns[j]->serialize_value_into_arena(i, pool, begin).size;
        }

        return {begin, sum_size};
    }

    size_t estimated_size(const ColumnRawPtrs& key_columns, uint32_t num_rows, bool is_join,
                          bool is_build, uint32_t bucket_size) override {
        size_t size = 0;
        for (const auto& column : key_columns) {
            size += column->byte_size();
        }

        size += sizeof(StringRef) * num_rows; // stored_keys
        if (is_join) {
            size += sizeof(uint32_t) * num_rows; // bucket_nums
        } else {
            size += sizeof(size_t) * num_rows; // hash_values
        }
        return size;
    }

    void init_serialized_keys_impl(const ColumnRawPtrs& key_columns, uint32_t num_rows,
                                   DorisVector<StringRef>& input_keys, Arena& input_arena) {
        input_arena.clear();
        input_keys.resize(num_rows);

        size_t max_one_row_byte_size = 0;
        for (const auto& column : key_columns) {
            max_one_row_byte_size += column->get_max_row_byte_size();
        }
        size_t total_bytes = max_one_row_byte_size * num_rows;
        if (total_bytes > config::pre_serialize_keys_limit_bytes) {
            // reach mem limit, don't serialize in batch
            size_t keys_size = key_columns.size();
            for (size_t i = 0; i < num_rows; ++i) {
                input_keys[i] =
                        serialize_keys_to_pool_contiguous(i, keys_size, key_columns, input_arena);
            }
        } else {
            auto* serialized_key_buffer =
                    reinterpret_cast<uint8_t*>(input_arena.alloc(total_bytes));

            for (size_t i = 0; i < num_rows; ++i) {
                input_keys[i].data =
                        reinterpret_cast<char*>(serialized_key_buffer + i * max_one_row_byte_size);
                input_keys[i].size = 0;
            }

            for (const auto& column : key_columns) {
                column->serialize(input_keys.data(), num_rows);
            }
        }
        Base::keys = input_keys.data();
    }

    size_t serialized_keys_size(bool is_build) const override {
        if (is_build) {
            return build_stored_keys.size() * sizeof(StringRef) + build_arena.size();
        } else {
            return stored_keys.size() * sizeof(StringRef) + Base::arena.size();
        }
    }

    void init_serialized_keys(const ColumnRawPtrs& key_columns, uint32_t num_rows,
                              const uint8_t* null_map = nullptr, bool is_join = false,
                              bool is_build = false, uint32_t bucket_size = 0) override {
        init_serialized_keys_impl(key_columns, num_rows, is_build ? build_stored_keys : stored_keys,
                                  is_build ? build_arena : Base::arena);
        if (is_join) {
            Base::init_join_bucket_num(num_rows, bucket_size, null_map);
        } else {
            Base::init_hash_values(num_rows, null_map);
        }
    }

    void insert_keys_into_columns(std::vector<StringRef>& input_keys, MutableColumns& key_columns,
                                  const uint32_t num_rows) override {
        for (auto& column : key_columns) {
            column->deserialize(input_keys.data(), num_rows);
        }
    }
};

/// Sub-table group indices for StringHashTable batch operations.
/// StringHashTable dispatches keys to 6 sub-tables by string length:
///   group 0: empty strings (size == 0) → m0
///   group 1: size <= 2 → m1
///   group 2: size <= 4 → m2
///   group 3: size <= 8 → m3
///   group 4: size <= 16 → m4
///   group 5: size > 16 or trailing zero → ms
/// By pre-grouping row indices, we can process each sub-table in a batch,
/// achieving better cache locality and enabling prefetch within each group.
struct StringKeySubTableGroups {
    static constexpr int NUM_GROUPS = 6;
    // Row indices for each sub-table group
    DorisVector<uint32_t> group_row_indices[NUM_GROUPS];

    void build(const StringRef* keys, uint32_t num_rows) {
        for (int g = 0; g < NUM_GROUPS; g++) {
            group_row_indices[g].clear();
        }
        // First pass: count sizes for each group to reserve memory
        uint32_t counts[NUM_GROUPS] = {};
        for (uint32_t i = 0; i < num_rows; i++) {
            counts[get_group(keys[i])]++;
        }
        for (int g = 0; g < NUM_GROUPS; g++) {
            group_row_indices[g].reserve(counts[g]);
        }
        // Second pass: fill group indices
        for (uint32_t i = 0; i < num_rows; i++) {
            group_row_indices[get_group(keys[i])].push_back(i);
        }
    }

    static ALWAYS_INLINE int get_group(const StringRef& key) {
        const size_t sz = key.size;
        if (sz == 0) {
            return 0;
        }
        if (key.data[sz - 1] == 0) {
            // Trailing zero: goes to the generic long-string table (ms)
            return 5;
        }
        if (sz <= 2) return 1;
        if (sz <= 4) return 2;
        if (sz <= 8) return 3;
        if (sz <= 16) return 4;
        return 5;
    }
};

template <typename TData>
struct MethodStringNoCache : public MethodBase<TData> {
    using Base = MethodBase<TData>;
    using Base::init_iterator;
    using Base::hash_table;
    using State =
            ColumnsHashing::HashMethodString<typename Base::Value, typename Base::Mapped, true>;

    // need keep until the hash probe end.
    DorisVector<StringRef> _build_stored_keys;
    // refresh each time probe
    DorisVector<StringRef> _stored_keys;

    // Sub-table groups for batch operations (only used for non-join aggregation path)
    StringKeySubTableGroups _sub_table_groups;

    size_t serialized_keys_size(bool is_build) const override {
        return is_build ? (_build_stored_keys.size() * sizeof(StringRef))
                        : (_stored_keys.size() * sizeof(StringRef));
    }

    size_t estimated_size(const ColumnRawPtrs& key_columns, uint32_t num_rows, bool is_join,
                          bool is_build, uint32_t bucket_size) override {
        size_t size = 0;
        size += sizeof(StringRef) * num_rows; // stored_keys
        if (is_join) {
            size += sizeof(uint32_t) * num_rows; // bucket_nums
        } else {
            size += sizeof(size_t) * num_rows; // hash_values
        }
        return size;
    }

    void init_serialized_keys_impl(const ColumnRawPtrs& key_columns, uint32_t num_rows,
                                   DorisVector<StringRef>& stored_keys) {
        const IColumn& column = *key_columns[0];
        const auto& nested_column =
                column.is_nullable()
                        ? assert_cast<const ColumnNullable&>(column).get_nested_column()
                        : column;
        auto serialized_str = [](const auto& column_string, DorisVector<StringRef>& stored_keys) {
            const auto& offsets = column_string.get_offsets();
            const auto* chars = column_string.get_chars().data();
            stored_keys.resize(column_string.size());
            for (size_t row = 0; row < column_string.size(); row++) {
                stored_keys[row] =
                        StringRef(chars + offsets[row - 1], offsets[row] - offsets[row - 1]);
            }
        };
        if (nested_column.is_column_string64()) {
            const auto& column_string = assert_cast<const ColumnString64&>(nested_column);
            serialized_str(column_string, stored_keys);
        } else {
            const auto& column_string = assert_cast<const ColumnString&>(nested_column);
            serialized_str(column_string, stored_keys);
        }
        Base::keys = stored_keys.data();
    }

    void init_serialized_keys(const ColumnRawPtrs& key_columns, uint32_t num_rows,
                              const uint8_t* null_map = nullptr, bool is_join = false,
                              bool is_build = false, uint32_t bucket_size = 0) override {
        init_serialized_keys_impl(key_columns, num_rows,
                                  is_build ? _build_stored_keys : _stored_keys);
        if (is_join) {
            Base::init_join_bucket_num(num_rows, bucket_size, null_map);
        } else {
            Base::init_hash_values(num_rows, null_map);
            // Build sub-table groups for batch emplace/find (only for aggregation, not join)
            if constexpr (Base::is_string_hash_map()) {
                _sub_table_groups.build(Base::keys, num_rows);
            }
        }
    }

    void insert_keys_into_columns(std::vector<StringRef>& input_keys, MutableColumns& key_columns,
                                  const uint32_t num_rows) override {
        key_columns[0]->reserve(num_rows);
        key_columns[0]->insert_many_strings(input_keys.data(), num_rows);
    }

    const StringKeySubTableGroups& get_sub_table_groups() const { return _sub_table_groups; }
};

/// Helper: detect whether HashMap is a nullable-wrapped StringHashMap.
template <typename HashMap>
struct IsNullableStringHashMap : std::false_type {};

template <typename Mapped, typename Allocator>
struct IsNullableStringHashMap<DataWithNullKey<StringHashMap<Mapped, Allocator>>> : std::true_type {
};

/// Helper: get the underlying StringHashTable from a hash table (handles DataWithNullKey wrapper).
template <typename HashMap>
auto& get_string_hash_table(HashMap& data) {
    return data;
}

/// Compile-time key conversion for each sub-table group.
/// Groups 1-4 use to_string_key<T>(); groups 0 and 5 use StringRef directly.
/// Returns the converted key for the given group.
/// For groups 0 and 5, the key is returned as a non-const copy (lazy_emplace_if_zero takes Key&).
template <int GroupIdx>
auto convert_key_for_submap(const StringRef& origin) {
    if constexpr (GroupIdx == 0) {
        return StringRef(origin); // copy — m0 needs non-const Key&
    } else if constexpr (GroupIdx == 1) {
        return to_string_key<StringKey2>(origin);
    } else if constexpr (GroupIdx == 2) {
        return to_string_key<StringKey4>(origin);
    } else if constexpr (GroupIdx == 3) {
        return to_string_key<StringKey8>(origin);
    } else if constexpr (GroupIdx == 4) {
        return to_string_key<StringKey16>(origin);
    } else {
        return StringRef(origin); // copy — ms uses StringRef as Key
    }
}

/// Hash value to use for a given group. Group 0 (empty string) always uses hash=0.
template <int GroupIdx, typename HashValues>
size_t hash_for_group(const HashValues& hash_values, uint32_t row) {
    if constexpr (GroupIdx == 0) {
        return 0;
    } else {
        return hash_values[row];
    }
}

/// Whether prefetch is useful for a group. Group 0 (StringHashTableEmpty, at most 1 element)
/// does not benefit from prefetch.
template <int GroupIdx>
static constexpr bool group_needs_prefetch = (GroupIdx != 0);

/// Process one sub-table group for emplace with result_handler.
/// Handles nullable null-key check, prefetch, key conversion, and emplace.
/// pre_handler(row) is called before each emplace, allowing callers to set per-row state
/// (e.g., current row index used inside creator lambdas).
template <int GroupIdx, bool is_nullable, typename Submap, typename HashMethodType, typename State,
          typename HashMap, typename F, typename FF, typename PreHandler, typename ResultHandler>
void process_submap_emplace(Submap& submap, const uint32_t* indices, size_t count,
                            HashMethodType& agg_method, State& state, HashMap& hash_table,
                            F&& creator, FF&& creator_for_null_key, PreHandler&& pre_handler,
                            ResultHandler&& result_handler) {
    using Mapped = typename HashMethodType::Mapped;
    for (size_t j = 0; j < count; j++) {
        if constexpr (group_needs_prefetch<GroupIdx>) {
            if (j + HASH_MAP_PREFETCH_DIST < count) {
                submap.template prefetch<false>(
                        agg_method.hash_values[indices[j + HASH_MAP_PREFETCH_DIST]]);
            }
        }
        uint32_t row = indices[j];
        pre_handler(row);
        if constexpr (is_nullable) {
            if (state.key_column->is_null_at(row)) {
                bool has_null_key = hash_table.has_null_key_data();
                hash_table.has_null_key_data() = true;
                if (!has_null_key) {
                    std::forward<FF>(creator_for_null_key)(
                            hash_table.template get_null_key_data<Mapped>());
                }
                result_handler(row, hash_table.template get_null_key_data<Mapped>());
                continue;
            }
        }
        auto origin = agg_method.keys[row];
        auto converted_key = convert_key_for_submap<GroupIdx>(origin);
        typename Submap::LookupResult result;
        if constexpr (GroupIdx == 0 || GroupIdx == 5) {
            // Groups 0,5: key and origin are the same StringRef
            submap.lazy_emplace_with_origin(converted_key, converted_key, result,
                                            hash_for_group<GroupIdx>(agg_method.hash_values, row),
                                            std::forward<F>(creator));
        } else {
            // Groups 1-4: converted_key differs from origin
            submap.lazy_emplace_with_origin(converted_key, origin, result,
                                            hash_for_group<GroupIdx>(agg_method.hash_values, row),
                                            std::forward<F>(creator));
        }
        result_handler(row, result->get_second());
    }
}

/// Process one sub-table group for emplace without result_handler (void version).
/// pre_handler(row) is called before each emplace.
template <int GroupIdx, bool is_nullable, typename Submap, typename HashMethodType, typename State,
          typename HashMap, typename F, typename FF, typename PreHandler>
void process_submap_emplace_void(Submap& submap, const uint32_t* indices, size_t count,
                                 HashMethodType& agg_method, State& state, HashMap& hash_table,
                                 F&& creator, FF&& creator_for_null_key, PreHandler&& pre_handler) {
    for (size_t j = 0; j < count; j++) {
        if constexpr (group_needs_prefetch<GroupIdx>) {
            if (j + HASH_MAP_PREFETCH_DIST < count) {
                submap.template prefetch<false>(
                        agg_method.hash_values[indices[j + HASH_MAP_PREFETCH_DIST]]);
            }
        }
        uint32_t row = indices[j];
        pre_handler(row);
        if constexpr (is_nullable) {
            if (state.key_column->is_null_at(row)) {
                bool has_null_key = hash_table.has_null_key_data();
                hash_table.has_null_key_data() = true;
                if (!has_null_key) {
                    std::forward<FF>(creator_for_null_key)();
                }
                continue;
            }
        }
        auto origin = agg_method.keys[row];
        auto converted_key = convert_key_for_submap<GroupIdx>(origin);
        typename Submap::LookupResult result;
        if constexpr (GroupIdx == 0 || GroupIdx == 5) {
            submap.lazy_emplace_with_origin(converted_key, converted_key, result,
                                            hash_for_group<GroupIdx>(agg_method.hash_values, row),
                                            std::forward<F>(creator));
        } else {
            submap.lazy_emplace_with_origin(converted_key, origin, result,
                                            hash_for_group<GroupIdx>(agg_method.hash_values, row),
                                            std::forward<F>(creator));
        }
    }
}

/// Process one sub-table group for find with result_handler.
template <int GroupIdx, bool is_nullable, typename Submap, typename HashMethodType, typename State,
          typename HashMap, typename ResultHandler>
void process_submap_find(Submap& submap, const uint32_t* indices, size_t count,
                         HashMethodType& agg_method, State& state, HashMap& hash_table,
                         ResultHandler&& result_handler) {
    using Mapped = typename HashMethodType::Mapped;
    using FindResult = typename ColumnsHashing::columns_hashing_impl::FindResultImpl<Mapped>;
    for (size_t j = 0; j < count; j++) {
        if constexpr (group_needs_prefetch<GroupIdx>) {
            if (j + HASH_MAP_PREFETCH_DIST < count) {
                submap.template prefetch<true>(
                        agg_method.hash_values[indices[j + HASH_MAP_PREFETCH_DIST]]);
            }
        }
        uint32_t row = indices[j];
        if constexpr (is_nullable) {
            if (state.key_column->is_null_at(row)) {
                if (hash_table.has_null_key_data()) {
                    FindResult res(&hash_table.template get_null_key_data<Mapped>(), true);
                    result_handler(row, res);
                } else {
                    FindResult res(nullptr, false);
                    result_handler(row, res);
                }
                continue;
            }
        }
        auto converted_key = convert_key_for_submap<GroupIdx>(agg_method.keys[row]);
        auto hash = hash_for_group<GroupIdx>(agg_method.hash_values, row);
        auto it = submap.find(converted_key, hash);
        if (it) {
            FindResult res(&it->get_second(), true);
            result_handler(row, res);
        } else {
            FindResult res(nullptr, false);
            result_handler(row, res);
        }
    }
}

/// Batch emplace helper: for StringHashMap, directly accesses sub-tables bypassing dispatch();
/// for other hash maps, does per-row loop with standard prefetch.
/// pre_handler(row) is called before each emplace, allowing callers to set per-row state
/// (e.g., current row index used inside creator lambdas).
/// result_handler(row_index, mapped) is called after each emplace.
template <typename HashMethodType, typename State, typename F, typename FF, typename PreHandler,
          typename ResultHandler>
void lazy_emplace_batch(HashMethodType& agg_method, State& state, uint32_t num_rows, F&& creator,
                        FF&& creator_for_null_key, PreHandler&& pre_handler,
                        ResultHandler&& result_handler) {
    if constexpr (HashMethodType::is_string_hash_map()) {
        using HashMap = typename HashMethodType::HashMapType;
        constexpr bool is_nullable = IsNullableStringHashMap<HashMap>::value;

        auto& hash_table = *agg_method.hash_table;
        auto& sht = get_string_hash_table(hash_table);
        const auto& groups = agg_method.get_sub_table_groups();

        sht.visit_submaps([&](auto group_idx, auto& submap) {
            constexpr int G = decltype(group_idx)::value;
            const auto& indices = groups.group_row_indices[G];
            if (!indices.empty()) {
                process_submap_emplace<G, is_nullable>(
                        submap, indices.data(), indices.size(), agg_method, state, hash_table,
                        creator, creator_for_null_key, pre_handler, result_handler);
            }
        });
    } else {
        // Standard per-row loop with ahead prefetch
        for (uint32_t i = 0; i < num_rows; ++i) {
            agg_method.template prefetch<false>(i);
            pre_handler(i);
            result_handler(i, *state.lazy_emplace_key(*agg_method.hash_table, i, agg_method.keys[i],
                                                      agg_method.hash_values[i], creator,
                                                      creator_for_null_key));
        }
    }
}

/// Convenience overload without pre_handler (uses no-op).
template <typename HashMethodType, typename State, typename F, typename FF, typename ResultHandler>
void lazy_emplace_batch(HashMethodType& agg_method, State& state, uint32_t num_rows, F&& creator,
                        FF&& creator_for_null_key, ResultHandler&& result_handler) {
    lazy_emplace_batch(
            agg_method, state, num_rows, std::forward<F>(creator),
            std::forward<FF>(creator_for_null_key), [](uint32_t) {},
            std::forward<ResultHandler>(result_handler));
}

/// Batch emplace helper (void version): like lazy_emplace_batch but ignores the return value.
/// pre_handler(row) is called before each emplace, allowing callers to update captured state
/// (e.g., the current row index used inside creator lambdas).
template <typename HashMethodType, typename State, typename F, typename FF, typename PreHandler>
void lazy_emplace_batch_void(HashMethodType& agg_method, State& state, uint32_t num_rows,
                             F&& creator, FF&& creator_for_null_key, PreHandler&& pre_handler) {
    if constexpr (HashMethodType::is_string_hash_map()) {
        using HashMap = typename HashMethodType::HashMapType;
        constexpr bool is_nullable = IsNullableStringHashMap<HashMap>::value;

        auto& hash_table = *agg_method.hash_table;
        auto& sht = get_string_hash_table(hash_table);
        const auto& groups = agg_method.get_sub_table_groups();

        sht.visit_submaps([&](auto group_idx, auto& submap) {
            constexpr int G = decltype(group_idx)::value;
            const auto& indices = groups.group_row_indices[G];
            if (!indices.empty()) {
                process_submap_emplace_void<G, is_nullable>(submap, indices.data(), indices.size(),
                                                            agg_method, state, hash_table, creator,
                                                            creator_for_null_key, pre_handler);
            }
        });
    } else {
        for (uint32_t i = 0; i < num_rows; ++i) {
            agg_method.template prefetch<false>(i);
            pre_handler(i);
            state.lazy_emplace_key(*agg_method.hash_table, i, agg_method.keys[i],
                                   agg_method.hash_values[i], creator, creator_for_null_key);
        }
    }
}

/// Batch find helper: for StringHashMap, directly accesses sub-tables bypassing dispatch();
/// for other hash maps, does per-row loop with standard prefetch.
template <typename HashMethodType, typename State, typename ResultHandler>
void find_batch(HashMethodType& agg_method, State& state, uint32_t num_rows,
                ResultHandler&& result_handler) {
    if constexpr (HashMethodType::is_string_hash_map()) {
        using HashMap = typename HashMethodType::HashMapType;
        constexpr bool is_nullable = IsNullableStringHashMap<HashMap>::value;

        auto& hash_table = *agg_method.hash_table;
        auto& sht = get_string_hash_table(hash_table);
        const auto& groups = agg_method.get_sub_table_groups();

        sht.visit_submaps([&](auto group_idx, auto& submap) {
            constexpr int G = decltype(group_idx)::value;
            const auto& indices = groups.group_row_indices[G];
            if (!indices.empty()) {
                process_submap_find<G, is_nullable>(submap, indices.data(), indices.size(),
                                                    agg_method, state, hash_table, result_handler);
            }
        });
    } else {
        for (uint32_t i = 0; i < num_rows; ++i) {
            agg_method.template prefetch<true>(i);
            auto find_result = state.find_key_with_hash(
                    *agg_method.hash_table, i, agg_method.keys[i], agg_method.hash_values[i]);
            result_handler(i, find_result);
        }
    }
}

/// For the case where there is one numeric key.
/// FieldType is UInt8/16/32/64 for any type with corresponding bit width.
template <typename FieldType, typename TData>
struct MethodOneNumber : public MethodBase<TData> {
    using Base = MethodBase<TData>;
    using Base::init_iterator;
    using Base::hash_table;
    using State = ColumnsHashing::HashMethodOneNumber<typename Base::Value, typename Base::Mapped,
                                                      FieldType>;

    size_t estimated_size(const ColumnRawPtrs& key_columns, uint32_t num_rows, bool is_join,
                          bool is_build, uint32_t bucket_size) override {
        size_t size = 0;
        if (is_join) {
            size += sizeof(uint32_t) * num_rows; // bucket_nums
        } else {
            size += sizeof(size_t) * num_rows; // hash_values
        }
        return size;
    }

    void init_serialized_keys(const ColumnRawPtrs& key_columns, uint32_t num_rows,
                              const uint8_t* null_map = nullptr, bool is_join = false,
                              bool is_build = false, uint32_t bucket_size = 0) override {
        Base::keys = (FieldType*)(key_columns[0]->is_nullable()
                                          ? assert_cast<const ColumnNullable*>(key_columns[0])
                                                    ->get_nested_column_ptr()
                                                    ->get_raw_data()
                                                    .data
                                          : key_columns[0]->get_raw_data().data);
        if (is_join) {
            Base::init_join_bucket_num(num_rows, bucket_size, null_map);
        } else {
            Base::init_hash_values(num_rows, null_map);
        }
    }

    void insert_keys_into_columns(std::vector<typename Base::Key>& input_keys,
                                  MutableColumns& key_columns, const uint32_t num_rows) override {
        if (!input_keys.empty()) {
            // If size() is ​0​, data() may or may not return a null pointer.
            key_columns[0]->insert_many_raw_data((char*)input_keys.data(), num_rows);
        }
    }
};

template <typename FieldType, typename TData>
struct MethodOneNumberDirect : public MethodOneNumber<FieldType, TData> {
    using Base = MethodOneNumber<FieldType, TData>;
    using Base::init_iterator;
    using Base::hash_table;
    using State = ColumnsHashing::HashMethodOneNumber<typename Base::Value, typename Base::Mapped,
                                                      FieldType>;
    FieldType _max_key;
    FieldType _min_key;

    MethodOneNumberDirect(FieldType max_key, FieldType min_key)
            : _max_key(max_key), _min_key(min_key) {}

    void init_serialized_keys(const ColumnRawPtrs& key_columns, uint32_t num_rows,
                              const uint8_t* null_map = nullptr, bool is_join = false,
                              bool is_build = false, uint32_t bucket_size = 0) override {
        Base::keys = (FieldType*)(key_columns[0]->is_nullable()
                                          ? assert_cast<const ColumnNullable*>(key_columns[0])
                                                    ->get_nested_column_ptr()
                                                    ->get_raw_data()
                                                    .data
                                          : key_columns[0]->get_raw_data().data);
        CHECK(is_join);
        CHECK_EQ(bucket_size, direct_mapping_range());
        Base::bucket_nums.resize(num_rows);

        if (null_map == nullptr) {
            if (is_build) {
                for (uint32_t k = 1; k < num_rows; ++k) {
                    Base::bucket_nums[k] = uint32_t(Base::keys[k] - _min_key + 1);
                }
            } else {
                for (uint32_t k = 0; k < num_rows; ++k) {
                    Base::bucket_nums[k] = (Base::keys[k] >= _min_key && Base::keys[k] <= _max_key)
                                                   ? uint32_t(Base::keys[k] - _min_key + 1)
                                                   : 0;
                }
            }
        } else {
            if (is_build) {
                for (uint32_t k = 1; k < num_rows; ++k) {
                    Base::bucket_nums[k] =
                            null_map[k] ? bucket_size : uint32_t(Base::keys[k] - _min_key + 1);
                }
            } else {
                for (uint32_t k = 0; k < num_rows; ++k) {
                    Base::bucket_nums[k] =
                            null_map[k] ? bucket_size
                            : (Base::keys[k] >= _min_key && Base::keys[k] <= _max_key)
                                    ? uint32_t(Base::keys[k] - _min_key + 1)
                                    : 0;
                }
            }
        }
    }

    uint32_t direct_mapping_range() override {
        // +2 to include max_key and one slot for out of range value
        return static_cast<uint32_t>(_max_key - _min_key + 2);
    }
};

template <int N>
void pack_nullmaps_interleaved(const uint8_t* const* datas, const uint8_t* bit_offsets,
                               size_t row_numbers, size_t stride, uint8_t* __restrict out) {
    static_assert(N >= 1 && N <= BITSIZE);

    const uint8_t* __restrict p0 = (N > 0) ? datas[0] : nullptr;
    const uint8_t* __restrict p1 = (N > 1) ? datas[1] : nullptr;
    const uint8_t* __restrict p2 = (N > 2) ? datas[2] : nullptr;
    const uint8_t* __restrict p3 = (N > 3) ? datas[3] : nullptr;
    const uint8_t* __restrict p4 = (N > 4) ? datas[4] : nullptr;
    const uint8_t* __restrict p5 = (N > 5) ? datas[5] : nullptr;
    const uint8_t* __restrict p6 = (N > 6) ? datas[6] : nullptr;
    const uint8_t* __restrict p7 = (N > 7) ? datas[7] : nullptr;

    const uint8_t m0 = (N > 0) ? bit_offsets[0] : 0;
    const uint8_t m1 = (N > 1) ? bit_offsets[1] : 0;
    const uint8_t m2 = (N > 2) ? bit_offsets[2] : 0;
    const uint8_t m3 = (N > 3) ? bit_offsets[3] : 0;
    const uint8_t m4 = (N > 4) ? bit_offsets[4] : 0;
    const uint8_t m5 = (N > 5) ? bit_offsets[5] : 0;
    const uint8_t m6 = (N > 6) ? bit_offsets[6] : 0;
    const uint8_t m7 = (N > 7) ? bit_offsets[7] : 0;

    for (size_t i = 0; i < row_numbers; ++i) {
        uint8_t byte = 0;

        if constexpr (N > 0) {
            byte |= p0[i] << m0;
        }
        if constexpr (N > 1) {
            byte |= p1[i] << m1;
        }
        if constexpr (N > 2) {
            byte |= p2[i] << m2;
        }
        if constexpr (N > 3) {
            byte |= p3[i] << m3;
        }
        if constexpr (N > 4) {
            byte |= p4[i] << m4;
        }
        if constexpr (N > 5) {
            byte |= p5[i] << m5;
        }
        if constexpr (N > 6) {
            byte |= p6[i] << m6;
        }
        if constexpr (N > 7) {
            byte |= p7[i] << m7;
        }

        out[i * stride] |= byte;
    }
}

template <int N>
struct PackNullmapsReducer {
    static void run(const uint8_t* const* datas, const uint8_t* coefficients, size_t row_numbers,
                    size_t stride, uint8_t* __restrict out) {
        pack_nullmaps_interleaved<N>(datas, coefficients, row_numbers, stride, out);
    }
};

template <typename TData>
struct MethodKeysFixed : public MethodBase<TData> {
    using Base = MethodBase<TData>;
    using typename Base::Key;
    using typename Base::Mapped;
    using Base::keys;
    using Base::hash_table;

    using State = ColumnsHashing::HashMethodKeysFixed<typename Base::Value, Key, Mapped>;

    // need keep until the hash probe end. use only in join
    DorisVector<Key> build_stored_keys;
    // refresh each time probe hash table
    DorisVector<Key> stored_keys;
    Sizes key_sizes;

    MethodKeysFixed(Sizes key_sizes_) : key_sizes(std::move(key_sizes_)) {}

    template <typename T>
    void pack_fixeds(size_t row_numbers, const ColumnRawPtrs& key_columns,
                     const ColumnRawPtrs& nullmap_columns, DorisVector<T>& result) {
        size_t bitmap_size = nullmap_columns.empty() ? 0 : 1;
        if (bitmap_size) {
            // set size to 0 at first, then use resize to call default constructor on index included from [0, row_numbers) to reset all memory
            // only need to reset the memory used to bitmap
            result.clear();
        }
        result.resize(row_numbers);

        auto* __restrict result_data = reinterpret_cast<char*>(result.data());

        size_t offset = 0;
        std::vector<bool> has_null_column(nullmap_columns.size(), false);
        if (bitmap_size > 0) {
            std::vector<const uint8_t*> nullmap_datas;
            std::vector<uint8_t> bit_offsets;
            for (size_t j = 0; j < nullmap_columns.size(); j++) {
                if (!nullmap_columns[j]) {
                    continue;
                }
                const uint8_t* __restrict data =
                        assert_cast<const ColumnUInt8&>(*nullmap_columns[j]).get_data().data();

                has_null_column[j] = simd::contain_one(data, row_numbers);
                if (has_null_column[j]) {
                    nullmap_datas.emplace_back(data);
                    bit_offsets.emplace_back(j);
                }
            }
            constexpr_int_match<1, BITSIZE, PackNullmapsReducer>::run(
                    int(nullmap_datas.size()), nullmap_datas.data(), bit_offsets.data(),
                    row_numbers, sizeof(T), reinterpret_cast<uint8_t*>(result_data));
            offset += bitmap_size;
        }

        for (size_t j = 0; j < key_columns.size(); ++j) {
            const char* __restrict data = key_columns[j]->get_raw_data().data;

            auto goo = [&]<typename Fixed, bool aligned>(Fixed zero) {
                CHECK_EQ(sizeof(Fixed), key_sizes[j]);
                if (has_null_column.size() && has_null_column[j]) {
                    const auto* nullmap =
                            assert_cast<const ColumnUInt8&>(*nullmap_columns[j]).get_data().data();
                    // make sure null cell is filled by 0x0
                    key_columns[j]->assume_mutable()->replace_column_null_data(nullmap);
                }
                auto* __restrict current = result_data + offset;
                for (size_t i = 0; i < row_numbers; ++i) {
                    memcpy_fixed<Fixed, aligned>(current, data);
                    current += sizeof(T);
                    data += sizeof(Fixed);
                }
            };
            auto foo = [&]<typename Fixed>(Fixed zero) {
                // Check alignment of both destination and source pointers.
                // Also verify that the stride sizeof(T) is a multiple of alignof(Fixed),
                // otherwise alignment will be lost on subsequent loop iterations
                // (e.g. UInt96 has sizeof=12, stride 12 is not a multiple of alignof(uint64_t)=8).
                if (sizeof(T) % alignof(Fixed) == 0 &&
                    reinterpret_cast<uintptr_t>(result_data + offset) % alignof(Fixed) == 0 &&
                    reinterpret_cast<uintptr_t>(data) % alignof(Fixed) == 0) {
                    goo.template operator()<Fixed, true>(zero);
                } else {
                    goo.template operator()<Fixed, false>(zero);
                }
            };

            if (key_sizes[j] == sizeof(uint8_t)) {
                foo(uint8_t());
            } else if (key_sizes[j] == sizeof(uint16_t)) {
                foo(uint16_t());
            } else if (key_sizes[j] == sizeof(uint32_t)) {
                foo(uint32_t());
            } else if (key_sizes[j] == sizeof(uint64_t)) {
                foo(uint64_t());
            } else if (key_sizes[j] == sizeof(UInt128)) {
                foo(UInt128());
            } else {
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "pack_fixeds input invalid key size, key_size={}", key_sizes[j]);
            }
            offset += key_sizes[j];
        }
    }

    size_t serialized_keys_size(bool is_build) const override {
        return (is_build ? build_stored_keys.size() : stored_keys.size()) *
               sizeof(typename Base::Key);
    }

    size_t estimated_size(const ColumnRawPtrs& key_columns, uint32_t num_rows, bool is_join,
                          bool is_build, uint32_t bucket_size) override {
        size_t size = 0;
        size += sizeof(StringRef) * num_rows; // stored_keys
        if (is_join) {
            size += sizeof(uint32_t) * num_rows; // bucket_nums
        } else {
            size += sizeof(size_t) * num_rows; // hash_values
        }
        return size;
    }

    void init_serialized_keys(const ColumnRawPtrs& key_columns, uint32_t num_rows,
                              const uint8_t* null_map = nullptr, bool is_join = false,
                              bool is_build = false, uint32_t bucket_size = 0) override {
        CHECK(key_columns.size() <= BITSIZE);
        ColumnRawPtrs actual_columns;
        ColumnRawPtrs null_maps;
        actual_columns.reserve(key_columns.size());
        null_maps.reserve(key_columns.size());
        bool has_nullable_key = false;

        for (const auto& col : key_columns) {
            if (const auto* nullable_col = check_and_get_column<ColumnNullable>(col)) {
                actual_columns.push_back(&nullable_col->get_nested_column());
                null_maps.push_back(&nullable_col->get_null_map_column());
                has_nullable_key = true;
            } else {
                actual_columns.push_back(col);
                null_maps.push_back(nullptr);
            }
        }
        if (!has_nullable_key) {
            null_maps.clear();
        }

        if (is_build) {
            pack_fixeds<Key>(num_rows, actual_columns, null_maps, build_stored_keys);
            Base::keys = build_stored_keys.data();
        } else {
            pack_fixeds<Key>(num_rows, actual_columns, null_maps, stored_keys);
            Base::keys = stored_keys.data();
        }

        if (is_join) {
            Base::init_join_bucket_num(num_rows, bucket_size, null_map);
        } else {
            Base::init_hash_values(num_rows, null_map);
        }
    }

    void insert_keys_into_columns(std::vector<typename Base::Key>& input_keys,
                                  MutableColumns& key_columns, const uint32_t num_rows) override {
        if (num_rows == 0) {
            return;
        }
        size_t pos = std::ranges::any_of(key_columns,
                                         [](const auto& col) { return col->is_nullable(); });

        for (size_t i = 0; i < key_columns.size(); ++i) {
            size_t size = key_sizes[i];
            char* data = nullptr;
            key_columns[i]->resize(num_rows);
            // If we have a nullable column, get its nested column and its null map.
            if (is_column_nullable(*key_columns[i])) {
                auto& nullable_col = assert_cast<ColumnNullable&>(*key_columns[i]);

                // nullable_col is obtained via key_columns and is itself a mutable element. However, when accessed
                // through get_raw_data().data, it yields a const char*, necessitating the use of const_cast.
                data = const_cast<char*>(nullable_col.get_nested_column().get_raw_data().data);
                UInt8* nullmap = assert_cast<ColumnUInt8*>(&nullable_col.get_null_map_column())
                                         ->get_data()
                                         .data();

                // The current column is nullable. Check if the value of the
                // corresponding key is nullable. Update the null map accordingly.
                for (size_t j = 0; j < num_rows; j++) {
                    nullmap[j] = (*reinterpret_cast<const UInt8*>(&input_keys[j]) >> i) & 1;
                }
            } else {
                // key_columns is a mutable element. However, when accessed through get_raw_data().data,
                // it yields a const char*, necessitating the use of const_cast.
                data = const_cast<char*>(key_columns[i]->get_raw_data().data);
            }

            auto goo = [&]<typename Fixed, bool aligned>(Fixed zero) {
                CHECK_EQ(sizeof(Fixed), size);
                for (size_t j = 0; j < num_rows; j++) {
                    memcpy_fixed<Fixed, aligned>(data + j * sizeof(Fixed),
                                                 (char*)(&input_keys[j]) + pos);
                }
            };
            auto foo = [&]<typename Fixed>(Fixed zero) {
                // Check alignment of both source and destination pointers.
                // The source steps by sizeof(Key) between iterations, so sizeof(Key)
                // must be a multiple of alignof(Fixed) to maintain alignment across
                // all iterations (e.g. UInt96 has sizeof=12, not a multiple of 8).
                if (sizeof(typename Base::Key) % alignof(Fixed) == 0 &&
                    reinterpret_cast<uintptr_t>((char*)(input_keys.data()) + pos) %
                                    alignof(Fixed) ==
                            0 &&
                    reinterpret_cast<uintptr_t>(data) % alignof(Fixed) == 0) {
                    goo.template operator()<Fixed, true>(zero);
                } else {
                    goo.template operator()<Fixed, false>(zero);
                }
            };

            if (size == sizeof(uint8_t)) {
                foo(uint8_t());
            } else if (size == sizeof(uint16_t)) {
                foo(uint16_t());
            } else if (size == sizeof(uint32_t)) {
                foo(uint32_t());
            } else if (size == sizeof(uint64_t)) {
                foo(uint64_t());
            } else if (size == sizeof(UInt128)) {
                foo(UInt128());
            } else {
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "pack_fixeds input invalid key size, key_size={}", size);
            }

            pos += size;
        }
    }
};

template <typename Base>
struct DataWithNullKey : public Base {
    bool& has_null_key_data() { return has_null_key; }
    bool has_null_key_data() const { return has_null_key; }
    template <typename MappedType>
    MappedType& get_null_key_data() const {
        return (MappedType&)null_key_data;
    }
    size_t size() const { return Base::size() + has_null_key; }
    bool empty() const { return Base::empty() && !has_null_key; }

    void clear() {
        Base::clear();
        has_null_key = false;
    }

    void clear_and_shrink() {
        Base::clear_and_shrink();
        has_null_key = false;
    }

protected:
    bool has_null_key = false;
    Base::Value null_key_data;
};

/// Single low cardinality column.
template <typename SingleColumnMethod>
struct MethodSingleNullableColumn : public SingleColumnMethod {
    using Base = SingleColumnMethod;
    using State = ColumnsHashing::HashMethodSingleLowNullableColumn<typename Base::State,
                                                                    typename Base::Mapped>;
    void insert_keys_into_columns(std::vector<typename Base::Key>& input_keys,
                                  MutableColumns& key_columns, const uint32_t num_rows) override {
        auto* col = key_columns[0].get();
        col->reserve(num_rows);
        if (input_keys.empty()) {
            // If size() is ​0​, data() may or may not return a null pointer.
            return;
        }
        if constexpr (std::is_same_v<typename Base::Key, StringRef>) {
            col->insert_many_strings(input_keys.data(), num_rows);
        } else {
            col->insert_many_raw_data(reinterpret_cast<char*>(input_keys.data()), num_rows);
        }
    }
};
} // namespace doris