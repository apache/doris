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

#include <memory>

#include "vec/columns/column_string.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/columns_hashing_impl.h"
#include "vec/common/hash_table/hash_table.h"
#include "vec/common/hash_table/hash_table_key_holder.h"
#include "vec/common/unaligned.h"

namespace doris::vectorized {

namespace ColumnsHashing {

/// For the case when there is one numeric key.
/// UInt8/16/32/64 for any type with corresponding bit width.
template <typename Value, typename Mapped, typename FieldType, bool use_cache = true>
struct HashMethodOneNumber : public columns_hashing_impl::HashMethodBase<
                                     HashMethodOneNumber<Value, Mapped, FieldType, use_cache>,
                                     Value, Mapped, use_cache> {
    using Self = HashMethodOneNumber<Value, Mapped, FieldType, use_cache>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache>;

    const char* vec;

    /// If the keys of a fixed length then key_sizes contains their lengths, empty otherwise.
    HashMethodOneNumber(const ColumnRawPtrs& key_columns, const Sizes& /*key_sizes*/,
                        const HashMethodContextPtr&) {
        vec = key_columns[0]->get_raw_data().data;
    }

    HashMethodOneNumber(const IColumn* column) { vec = column->get_raw_data().data; }

    /// Creates context. Method is called once and result context is used in all threads.
    using Base::createContext; /// (const HashMethodContext::Settings &) -> HashMethodContextPtr

    /// Emplace key into HashTable or HashMap. If Data is HashMap, returns ptr to value, otherwise nullptr.
    /// Data is a HashTable where to insert key from column's row.
    /// For Serialized method, key may be placed in pool.
    using Base::emplace_key; /// (Data & data, size_t row, Arena & pool) -> EmplaceResult

    /// Find key into HashTable or HashMap. If Data is HashMap and key was found, returns ptr to value, otherwise nullptr.
    using Base::find_key; /// (Data & data, size_t row, Arena & pool) -> FindResult

    /// Get hash value of row.
    using Base::get_hash; /// (const Data & data, size_t row, Arena & pool) -> size_t

    /// Is used for default implementation in HashMethodBase.
    FieldType get_key_holder(size_t row, Arena&) const {
        return unaligned_load<FieldType>(vec + row * sizeof(FieldType));
    }
};

/// For the case when there is one string key.
template <typename Value, typename Mapped, bool place_string_to_arena = true, bool use_cache = true>
struct HashMethodString : public columns_hashing_impl::HashMethodBase<
                                  HashMethodString<Value, Mapped, place_string_to_arena, use_cache>,
                                  Value, Mapped, use_cache> {
    using Self = HashMethodString<Value, Mapped, place_string_to_arena, use_cache>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache>;

    const IColumn::Offset* offsets;
    const UInt8* chars;

    HashMethodString(const ColumnRawPtrs& key_columns, const Sizes& /*key_sizes*/,
                     const HashMethodContextPtr&) {
        const IColumn& column = *key_columns[0];
        const ColumnString& column_string = assert_cast<const ColumnString&>(column);
        offsets = column_string.get_offsets().data();
        chars = column_string.get_chars().data();
    }

    auto get_key_holder(ssize_t row, [[maybe_unused]] Arena& pool) const {
        StringRef key(chars + offsets[row - 1], offsets[row] - offsets[row - 1] - 1);

        if constexpr (place_string_to_arena) {
            return ArenaKeyHolder{key, pool};
        } else {
            return key;
        }
    }

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache>;
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

    ColumnRawPtrs key_columns;
    size_t keys_size;

    HashMethodSerialized(const ColumnRawPtrs& key_columns_, const Sizes& /*key_sizes*/,
                         const HashMethodContextPtr&)
            : key_columns(key_columns_), keys_size(key_columns_.size()) {}

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;

    ALWAYS_INLINE SerializedKeyHolder get_key_holder(size_t row, Arena& pool) const {
        return SerializedKeyHolder{serialize_keys_to_pool_contiguous(row, keys_size, key_columns, pool),
                                   pool};
    }
};

/// For the case when there is one string key.
template <typename Value, typename Mapped, bool use_cache = true>
struct HashMethodHashed
        : public columns_hashing_impl::HashMethodBase<HashMethodHashed<Value, Mapped, use_cache>,
                                                      Value, Mapped, use_cache> {
    using Key = UInt128;
    using Self = HashMethodHashed<Value, Mapped, use_cache>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache>;

    ColumnRawPtrs key_columns;

    HashMethodHashed(ColumnRawPtrs key_columns_, const Sizes&, const HashMethodContextPtr&)
            : key_columns(std::move(key_columns_)) {}

    ALWAYS_INLINE Key get_key_holder(size_t row, Arena&) const {
        return hash128(row, key_columns.size(), key_columns);
    }
};

} // namespace ColumnsHashing
} // namespace doris::vectorized
