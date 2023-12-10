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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ColumnsHashing.h
// and modified by Doris

#pragma once

#include <memory>
#include <span>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_string.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/columns_hashing_impl.h"
#include "vec/common/hash_table/hash_table.h"
#include "vec/common/hash_table/ph_hash_map.h"
#include "vec/common/string_ref.h"
#include "vec/common/unaligned.h"

namespace doris::vectorized {

using Sizes = std::vector<size_t>;
namespace ColumnsHashing {

/// For the case when there is one numeric key.
/// UInt8/16/32/64 for any type with corresponding bit width.
template <typename Value, typename Mapped, typename FieldType>
struct HashMethodOneNumber
        : public columns_hashing_impl::HashMethodBase<HashMethodOneNumber<Value, Mapped, FieldType>,
                                                      Value, Mapped, false> {
    using Self = HashMethodOneNumber<Value, Mapped, FieldType>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;

    HashMethodOneNumber(const ColumnRawPtrs& key_columns) {}

    using Base::find_key_with_hash;
};

/// For the case when there is one string key.
template <typename Value, typename Mapped, bool place_string_to_arena = true>
struct HashMethodString
        : public columns_hashing_impl::HashMethodBase<
                  HashMethodString<Value, Mapped, place_string_to_arena>, Value, Mapped, false> {
    using Self = HashMethodString<Value, Mapped, place_string_to_arena>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;

    HashMethodString(const ColumnRawPtrs& key_columns) {}

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;
};

/** Hash by concatenating serialized key values.
  * The serialized value differs in that it uniquely allows to deserialize it, having only the position with which it starts.
  * That is, for example, for strings, it contains first the serialized length of the string, and then the bytes.
  * Therefore, when aggregating by several strings, there is no ambiguity.
  */
template <typename Value, typename Mapped>
struct HashMethodSerialized
        : public columns_hashing_impl::HashMethodBase<HashMethodSerialized<Value, Mapped>, Value,
                                                      Mapped, false> {
    using Self = HashMethodSerialized<Value, Mapped>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;

    HashMethodSerialized(const ColumnRawPtrs& key_columns) {}

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;
};

/// For the case when all keys are of fixed length, and they fit in N (for example, 128) bits.
template <typename Value, typename Key, typename Mapped, bool has_nullable_keys = false>
struct HashMethodKeysFixed
        : private columns_hashing_impl::BaseStateKeysFixed<Key, has_nullable_keys>,
          public columns_hashing_impl::HashMethodBase<
                  HashMethodKeysFixed<Value, Key, Mapped, has_nullable_keys>, Value, Mapped,
                  false> {
    using Self = HashMethodKeysFixed<Value, Key, Mapped, has_nullable_keys>;
    using BaseHashed = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;
    using Base = columns_hashing_impl::BaseStateKeysFixed<Key, has_nullable_keys>;

    HashMethodKeysFixed(const ColumnRawPtrs& key_columns) : Base(key_columns) {}
};

template <typename SingleColumnMethod, typename Mapped>
struct HashMethodSingleLowNullableColumn : public SingleColumnMethod {
    using Base = SingleColumnMethod;

    static constexpr bool has_mapped = !std::is_same<Mapped, void>::value;
    using FindResult = columns_hashing_impl::FindResultImpl<Mapped>;

    const ColumnNullable* key_column;

    static ColumnRawPtrs get_nested_column(const IColumn* col) {
        const auto* nullable = check_and_get_column<ColumnNullable>(*col);
        DCHECK(nullable != nullptr);
        const auto* const nested_col = nullable->get_nested_column_ptr().get();
        return {nested_col};
    }

    HashMethodSingleLowNullableColumn(const ColumnRawPtrs& key_columns_nullable)
            : Base(get_nested_column(key_columns_nullable[0])),
              key_column(assert_cast<const ColumnNullable*>(key_columns_nullable[0])) {}

    template <typename Data, typename Func, typename CreatorForNull, typename KeyHolder>
        requires has_mapped
    ALWAYS_INLINE Mapped& lazy_emplace_key(Data& data, size_t row, KeyHolder&& key,
                                           size_t hash_value, Func&& f,
                                           CreatorForNull&& null_creator) {
        if (key_column->is_null_at(row)) {
            bool has_null_key = data.has_null_key_data();
            data.has_null_key_data() = true;
            if (!has_null_key) {
                std::forward<CreatorForNull>(null_creator)(
                        data.template get_null_key_data<Mapped>());
            }
            return data.template get_null_key_data<Mapped>();
        }
        typename Data::LookupResult it;
        data.lazy_emplace(std::forward<KeyHolder>(key), it, hash_value, std::forward<Func>(f));
        return *lookup_result_get_mapped(it);
    }
};

} // namespace ColumnsHashing
} // namespace doris::vectorized
