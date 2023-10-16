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

#include <type_traits>
#include <utility>

#include "runtime/descriptors.h"
#include "util/stack_util.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/columns_hashing.h"
#include "vec/common/hash_table/partitioned_hash_map.h"
#include "vec/common/hash_table/string_hash_map.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

constexpr auto BITSIZE = 8;

template <typename Base>
struct DataWithNullKey;

template <typename HashMap>
struct MethodBase {
    using Key = typename HashMap::key_type;
    using Mapped = typename HashMap::mapped_type;
    using Value = typename HashMap::value_type;
    using Iterator = typename HashMap::iterator;
    using HashMapType = HashMap;

    std::shared_ptr<HashMap> hash_table;
    Iterator iterator;
    bool inited_iterator = false;
    Key* keys;
    Arena arena;
    std::vector<size_t> hash_values;

    MethodBase() { hash_table.reset(new HashMap()); }
    virtual ~MethodBase() = default;

    virtual void reset() {
        arena.clear();
        inited_iterator = false;
    }

    void init_iterator() {
        if (!inited_iterator) {
            inited_iterator = true;
            iterator = hash_table->begin();
        }
    }
    virtual void init_serialized_keys(const ColumnRawPtrs& key_columns, size_t num_rows,
                                      const uint8_t* null_map = nullptr) = 0;

    void init_hash_values(size_t num_rows, const uint8_t* null_map) {
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
    void init_hash_values(size_t num_rows) {
        hash_values.resize(num_rows);
        for (size_t k = 0; k < num_rows; ++k) {
            hash_values[k] = hash_table->hash(keys[k]);
        }
    }

    template <bool read>
    void prefetch(int currrent) {
        if (LIKELY(currrent + HASH_MAP_PREFETCH_DIST < hash_values.size())) {
            hash_table->template prefetch<read>(keys[currrent + HASH_MAP_PREFETCH_DIST],
                                                hash_values[currrent + HASH_MAP_PREFETCH_DIST]);
        }
    }

    template <typename State>
    auto find(State& state, size_t i) {
        prefetch<true>(i);
        return state.find_key_with_hash(*hash_table, hash_values[i], keys[i]);
    }

    template <typename State, typename F, typename FF>
    auto& lazy_emplace(State& state, size_t i, F&& creator, FF&& creator_for_null_key) {
        prefetch<false>(i);
        return state.lazy_emplace_key(*hash_table, i, keys[i], hash_values[i], creator,
                                      creator_for_null_key);
    }

    static constexpr bool need_presis() { return std::is_same_v<Key, StringRef>; }

    static constexpr bool is_string_hash_map() {
        return std::is_same_v<StringHashMap<Mapped>, HashMap> ||
               std::is_same_v<DataWithNullKey<StringHashMap<Mapped>>, HashMap>;
    }

    template <typename Key, typename Origin>
    static void try_presis_key(Key& key, Origin& origin, Arena& arena) {
        if constexpr (need_presis()) {
            origin.data = arena.insert(origin.data, origin.size);
            if constexpr (!is_string_hash_map()) {
                key = origin;
            }
        }
    }

    virtual void insert_keys_into_columns(std::vector<Key>& keys, MutableColumns& key_columns,
                                          size_t num_rows) = 0;
};

template <typename TData>
struct MethodSerialized : public MethodBase<TData> {
    using Base = MethodBase<TData>;
    using Base::init_iterator;
    using State = ColumnsHashing::HashMethodSerialized<typename Base::Value, typename Base::Mapped>;
    using Base::try_presis_key;

    std::vector<StringRef> stored_keys;

    StringRef serialize_keys_to_pool_contiguous(size_t i, size_t keys_size,
                                                const ColumnRawPtrs& key_columns, Arena& pool) {
        const char* begin = nullptr;

        size_t sum_size = 0;
        for (size_t j = 0; j < keys_size; ++j) {
            sum_size += key_columns[j]->serialize_value_into_arena(i, pool, begin).size;
        }

        return {begin, sum_size};
    }

    void init_serialized_keys(const ColumnRawPtrs& key_columns, size_t num_rows,
                              const uint8_t* null_map = nullptr) override {
        Base::arena.clear();
        stored_keys.resize(num_rows);

        size_t max_one_row_byte_size = 0;
        for (const auto& column : key_columns) {
            max_one_row_byte_size += column->get_max_row_byte_size();
        }
        size_t total_bytes = max_one_row_byte_size * num_rows;

        if (total_bytes > config::pre_serialize_keys_limit_bytes) {
            // reach mem limit, don't serialize in batch
            size_t keys_size = key_columns.size();
            for (size_t i = 0; i < num_rows; ++i) {
                stored_keys[i] =
                        serialize_keys_to_pool_contiguous(i, keys_size, key_columns, Base::arena);
            }
        } else {
            auto* serialized_key_buffer =
                    reinterpret_cast<uint8_t*>(Base::arena.alloc(total_bytes));

            for (size_t i = 0; i < num_rows; ++i) {
                stored_keys[i].data =
                        reinterpret_cast<char*>(serialized_key_buffer + i * max_one_row_byte_size);
                stored_keys[i].size = 0;
            }

            for (const auto& column : key_columns) {
                column->serialize_vec(stored_keys, num_rows, max_one_row_byte_size);
            }
        }
        Base::keys = stored_keys.data();
        Base::init_hash_values(num_rows, null_map);
    }

    void insert_keys_into_columns(std::vector<StringRef>& keys, MutableColumns& key_columns,
                                  const size_t num_rows) override {
        for (auto& column : key_columns) {
            column->deserialize_vec(keys, num_rows);
        }
    }
};

inline size_t get_bitmap_size(size_t key_number) {
    return (key_number + BITSIZE - 1) / BITSIZE;
}

template <typename TData>
struct MethodStringNoCache : public MethodBase<TData> {
    using Base = MethodBase<TData>;
    using Base::init_iterator;
    using Base::hash_table;
    using State =
            ColumnsHashing::HashMethodString<typename Base::Value, typename Base::Mapped, true>;

    std::vector<StringRef> stored_keys;

    void init_serialized_keys(const ColumnRawPtrs& key_columns, size_t num_rows,
                              const uint8_t* null_map = nullptr) override {
        const IColumn& column = *key_columns[0];
        const auto& column_string = assert_cast<const ColumnString&>(
                column.is_nullable()
                        ? assert_cast<const ColumnNullable&>(column).get_nested_column()
                        : column);
        const auto* offsets = column_string.get_offsets().data();
        const auto* chars = column_string.get_chars().data();

        stored_keys.resize(column_string.size());
        for (size_t row = 0; row < column_string.size(); row++) {
            stored_keys[row] = StringRef(chars + offsets[row - 1], offsets[row] - offsets[row - 1]);
        }

        Base::keys = stored_keys.data();
        Base::init_hash_values(num_rows, null_map);
    }

    void insert_keys_into_columns(std::vector<StringRef>& keys, MutableColumns& key_columns,
                                  const size_t num_rows) override {
        key_columns[0]->reserve(num_rows);
        key_columns[0]->insert_many_strings(keys.data(), num_rows);
    }
};

/// For the case where there is one numeric key.
/// FieldType is UInt8/16/32/64 for any type with corresponding bit width.
template <typename FieldType, typename TData>
struct MethodOneNumber : public MethodBase<TData> {
    using Base = MethodBase<TData>;
    using Base::init_iterator;
    using Base::hash_table;
    using State = ColumnsHashing::HashMethodOneNumber<typename Base::Value, typename Base::Mapped,
                                                      FieldType>;

    void init_serialized_keys(const ColumnRawPtrs& key_columns, size_t num_rows,
                              const uint8_t* null_map = nullptr) override {
        Base::keys = (FieldType*)(key_columns[0]->is_nullable()
                                          ? assert_cast<const ColumnNullable*>(key_columns[0])
                                                    ->get_nested_column_ptr()
                                          : key_columns[0])
                             ->get_raw_data()
                             .data;
        std::string name = key_columns[0]->get_name();
        Base::init_hash_values(num_rows, null_map);
    }

    void insert_keys_into_columns(std::vector<typename Base::Key>& keys,
                                  MutableColumns& key_columns, const size_t num_rows) override {
        key_columns[0]->reserve(num_rows);
        auto* column = static_cast<ColumnVectorHelper*>(key_columns[0].get());
        for (size_t i = 0; i != num_rows; ++i) {
            const auto* key_holder = reinterpret_cast<const char*>(&keys[i]);
            column->insert_raw_data<sizeof(FieldType)>(key_holder);
        }
    }
};

template <typename TData, bool has_nullable_keys = false>
struct MethodKeysFixed : public MethodBase<TData> {
    using Base = MethodBase<TData>;
    using typename Base::Key;
    using typename Base::Mapped;
    using Base::keys;
    using Base::hash_table;
    using Base::iterator;

    using State = ColumnsHashing::HashMethodKeysFixed<typename Base::Value, Key, Mapped,
                                                      has_nullable_keys>;

    std::vector<Key> stored_keys;
    Sizes key_sizes;

    MethodKeysFixed(Sizes key_sizes_) : key_sizes(std::move(key_sizes_)) {}

    template <typename T>
    std::vector<T> pack_fixeds(size_t row_numbers, const ColumnRawPtrs& key_columns,
                               const ColumnRawPtrs& nullmap_columns) {
        size_t bitmap_size = get_bitmap_size(nullmap_columns.size());

        std::vector<T> result(row_numbers);
        size_t offset = 0;
        if (bitmap_size > 0) {
            for (size_t j = 0; j < nullmap_columns.size(); j++) {
                if (!nullmap_columns[j]) {
                    continue;
                }
                size_t bucket = j / BITSIZE;
                size_t offset = j % BITSIZE;
                const auto& data =
                        assert_cast<const ColumnUInt8&>(*nullmap_columns[j]).get_data().data();
                for (size_t i = 0; i < row_numbers; ++i) {
                    *((char*)(&result[i]) + bucket) |= data[i] << offset;
                }
            }
            offset += bitmap_size;
        }

        for (size_t j = 0; j < key_columns.size(); ++j) {
            const char* data = key_columns[j]->get_raw_data().data;

            auto foo = [&]<typename Fixed>(Fixed zero) {
                CHECK_EQ(sizeof(Fixed), key_sizes[j]);
                if (!nullmap_columns.empty() && nullmap_columns[j]) {
                    const auto& nullmap =
                            assert_cast<const ColumnUInt8&>(*nullmap_columns[j]).get_data().data();
                    for (size_t i = 0; i < row_numbers; ++i) {
                        // make sure null cell is filled by 0x0
                        memcpy_fixed<Fixed>((char*)(&result[i]) + offset,
                                            nullmap[i] ? (char*)&zero : data + i * sizeof(Fixed));
                    }
                } else {
                    for (size_t i = 0; i < row_numbers; ++i) {
                        memcpy_fixed<Fixed>((char*)(&result[i]) + offset, data + i * sizeof(Fixed));
                    }
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
        return result;
    }

    void init_serialized_keys(const ColumnRawPtrs& key_columns, size_t num_rows,
                              const uint8_t* null_map = nullptr) override {
        ColumnRawPtrs actual_columns;
        ColumnRawPtrs null_maps;
        if (has_nullable_keys) {
            actual_columns.reserve(key_columns.size());
            null_maps.reserve(key_columns.size());
            for (const auto& col : key_columns) {
                if (const auto* nullable_col = check_and_get_column<ColumnNullable>(col)) {
                    actual_columns.push_back(&nullable_col->get_nested_column());
                    null_maps.push_back(&nullable_col->get_null_map_column());
                } else {
                    actual_columns.push_back(col);
                    null_maps.push_back(nullptr);
                }
            }
        } else {
            actual_columns = key_columns;
        }
        stored_keys = pack_fixeds<Key>(num_rows, actual_columns, null_maps);
        Base::keys = stored_keys.data();
        Base::init_hash_values(num_rows, null_map);
    }

    void insert_keys_into_columns(std::vector<typename Base::Key>& keys,
                                  MutableColumns& key_columns, const size_t num_rows) override {
        // In any hash key value, column values to be read start just after the bitmap, if it exists.
        size_t pos = has_nullable_keys ? get_bitmap_size(key_columns.size()) : 0;

        for (size_t i = 0; i < key_columns.size(); ++i) {
            size_t size = key_sizes[i];
            char* data = nullptr;
            key_columns[i]->resize(num_rows);
            // If we have a nullable column, get its nested column and its null map.
            if (is_column_nullable(*key_columns[i])) {
                auto& nullable_col = assert_cast<ColumnNullable&>(*key_columns[i]);

                data = const_cast<char*>(nullable_col.get_nested_column().get_raw_data().data);
                UInt8* nullmap = assert_cast<ColumnUInt8*>(&nullable_col.get_null_map_column())
                                         ->get_data()
                                         .data();

                // The current column is nullable. Check if the value of the
                // corresponding key is nullable. Update the null map accordingly.
                size_t bucket = i / BITSIZE;
                size_t offset = i % BITSIZE;
                for (size_t j = 0; j < num_rows; j++) {
                    nullmap[j] = (reinterpret_cast<const UInt8*>(&keys[j])[bucket] >> offset) & 1;
                }
            } else {
                data = const_cast<char*>(key_columns[i]->get_raw_data().data);
            }

            auto foo = [&]<typename Fixed>(Fixed zero) {
                CHECK_EQ(sizeof(Fixed), size);
                for (size_t j = 0; j < num_rows; j++) {
                    memcpy_fixed<Fixed>(data + j * sizeof(Fixed), (char*)(&keys[j]) + pos);
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

private:
    bool has_null_key = false;
    // null_key_data store AggregateDataPtr on agg node, store PartitionBlocks on partition sort node.
    void* null_key_data = nullptr;
};

/// Single low cardinality column.
template <typename SingleColumnMethod>
struct MethodSingleNullableColumn : public SingleColumnMethod {
    using Base = SingleColumnMethod;
    using State = ColumnsHashing::HashMethodSingleLowNullableColumn<typename Base::State,
                                                                    typename Base::Mapped>;

    void insert_keys_into_columns(std::vector<typename Base::Key>& keys,
                                  MutableColumns& key_columns, const size_t num_rows) override {
        auto* col = key_columns[0].get();
        col->reserve(num_rows);
        if constexpr (std::is_same_v<typename Base::Key, StringRef>) {
            col->insert_many_strings(keys.data(), num_rows);
        } else {
            col->insert_many_raw_data(reinterpret_cast<char*>(keys.data()), num_rows);
        }
    }
};

template <typename RowRefListType>
using SerializedHashTableContext = MethodSerialized<JoinFixedHashMap<StringRef, RowRefListType>>;

template <class T, typename RowRefListType>
using PrimaryTypeHashTableContext =
        MethodOneNumber<T, JoinFixedHashMap<T, RowRefListType, HashCRC32<T>>>;

template <class Key, bool has_null, typename Value>
using FixedKeyHashTableContext =
        MethodKeysFixed<JoinFixedHashMap<Key, Value, HashCRC32<Key>>, has_null>;

} // namespace doris::vectorized