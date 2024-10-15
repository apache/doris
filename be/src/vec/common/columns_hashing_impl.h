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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ColumnsHashingImpl.h
// and modified by Doris

#pragma once

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/common/hash_table/ph_hash_map.h"

namespace doris::vectorized {

namespace ColumnsHashing {

namespace columns_hashing_impl {

template <typename Value, bool consecutive_keys_optimization_>
struct LastElementCache {
    static constexpr bool consecutive_keys_optimization = consecutive_keys_optimization_;
    Value value;
    bool empty = true;
    bool found = false;

    bool check(const Value& value_) { return !empty && value == value_; }

    template <typename Key>
    bool check(const Key& key) {
        return !empty && value.first == key;
    }
};

template <typename Data>
struct LastElementCache<Data, false> {
    static constexpr bool consecutive_keys_optimization = false;
};

template <typename Mapped>
class FindResultImpl {
    Mapped* value = nullptr;
    bool found;

public:
    FindResultImpl(Mapped* value_, bool found_) : value(value_), found(found_) {}
    bool is_found() const { return found; }
    Mapped& get_mapped() const { return *value; }
};

template <>
class FindResultImpl<void> {
    bool found;

public:
    explicit FindResultImpl(bool found_) : found(found_) {}
    bool is_found() const { return found; }
};

template <typename Derived, typename Value, typename Mapped, bool consecutive_keys_optimization>
class HashMethodBase {
public:
    using FindResult = FindResultImpl<Mapped>;
    static constexpr bool has_mapped = !std::is_same_v<Mapped, void>;
    using Cache = LastElementCache<Value, consecutive_keys_optimization>;

    template <typename Data, typename Func, typename CreatorForNull, typename KeyHolder>
        requires has_mapped
    ALWAYS_INLINE Mapped& lazy_emplace_key(Data& data, size_t row, KeyHolder&& key,
                                           size_t hash_value, Func&& f,
                                           CreatorForNull&& null_creator) {
        return lazy_emplace_impl(std::forward<KeyHolder>(key), hash_value, data,
                                 std::forward<Func>(f));
    }

    template <typename Data, typename Key>
    ALWAYS_INLINE FindResult find_key_with_hash(Data& data, size_t i, Key key, size_t hash_value) {
        return find_key_impl(key, hash_value, data);
    }

protected:
    Cache cache;

    HashMethodBase() {
        if constexpr (consecutive_keys_optimization) {
            if constexpr (has_mapped) {
                /// Init PairNoInit elements.
                cache.value.second = Mapped();
                cache.value.first = {};
            } else
                cache.value = Value();
        }
    }

    template <typename Data, typename KeyHolder, typename Func>
        requires has_mapped
    ALWAYS_INLINE Mapped& lazy_emplace_impl(KeyHolder& key_holder, size_t hash_value, Data& data,
                                            Func&& f) {
        typename Data::LookupResult it;
        data.lazy_emplace(key_holder, it, hash_value, std::forward<Func>(f));

        return *lookup_result_get_mapped(it);
    }

    template <typename Data, typename Key>
    ALWAYS_INLINE FindResult find_key_impl(Key key, size_t hash_value, Data& data) {
        if constexpr (Cache::consecutive_keys_optimization) {
            if (cache.check(key)) {
                if constexpr (has_mapped)
                    return FindResult(&cache.value.second, cache.found);
                else
                    return FindResult(cache.found);
            }
        }

        auto it = data.find(key, hash_value);

        if constexpr (consecutive_keys_optimization) {
            cache.found = it != nullptr;
            cache.empty = false;

            if constexpr (has_mapped) {
                cache.value.first = key;
                if (it) {
                    cache.value.second = *lookup_result_get_mapped(it);
                }
            } else {
                cache.value = key;
            }
        }

        if constexpr (has_mapped)
            return FindResult(it ? lookup_result_get_mapped(it) : nullptr, it != nullptr);
        else
            return FindResult(it != nullptr);
    }
};

template <typename T>
struct MappedCache : public PaddedPODArray<T> {};

template <>
struct MappedCache<void> {};

/// This class is designed to provide the functionality that is required for
/// supporting nullable keys in HashMethodKeysFixed. If there are
/// no nullable keys, this class is merely implemented as an empty shell.
template <typename Key, bool has_nullable_keys>
class BaseStateKeysFixed;

/// Case where nullable keys are supported.
template <typename Key>
class BaseStateKeysFixed<Key, true> {
protected:
    BaseStateKeysFixed(const ColumnRawPtrs& key_columns) {
        null_maps.reserve(key_columns.size());
        actual_columns.reserve(key_columns.size());

        for (const auto& col : key_columns) {
            if (auto* nullable_col = check_and_get_column<ColumnNullable>(col)) {
                actual_columns.push_back(&nullable_col->get_nested_column());
                null_maps.push_back(&nullable_col->get_null_map_column());
            } else {
                actual_columns.push_back(col);
                null_maps.push_back(nullptr);
            }
        }
    }

    /// Return the columns which actually contain the values of the keys.
    /// For a given key column, if it is nullable, we return its nested
    /// column. Otherwise we return the key column itself.
    const ColumnRawPtrs& get_actual_columns() const { return actual_columns; }

    const ColumnRawPtrs& get_nullmap_columns() const { return null_maps; }

private:
    ColumnRawPtrs actual_columns;
    ColumnRawPtrs null_maps;
};

/// Case where nullable keys are not supported.
template <typename Key>
class BaseStateKeysFixed<Key, false> {
protected:
    BaseStateKeysFixed(const ColumnRawPtrs& columns) : actual_columns(columns) {}

    const ColumnRawPtrs& get_actual_columns() const { return actual_columns; }

    const ColumnRawPtrs& get_nullmap_columns() const { return null_maps; }

private:
    ColumnRawPtrs actual_columns;
    ColumnRawPtrs null_maps;
};

} // namespace columns_hashing_impl

} // namespace ColumnsHashing

} // namespace doris::vectorized
