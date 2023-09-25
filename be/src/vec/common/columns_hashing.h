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
#include "vec/common/hash_table/hash_table_key_holder.h"
#include "vec/common/hash_table/ph_hash_map.h"
#include "vec/common/string_ref.h"
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
    size_t size;

    /// If the keys of a fixed length then key_sizes contains their lengths, empty otherwise.
    HashMethodOneNumber(const ColumnRawPtrs& key_columns, const Sizes& /*key_sizes*/,
                        const HashMethodContextPtr&) {
        vec = key_columns[0]->get_raw_data().data;
        size = key_columns[0]->size();
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
    using Base::find_key_with_hash;

    /// Is used for default implementation in HashMethodBase.
    FieldType get_key_holder(size_t row, Arena&) const { return ((FieldType*)(vec))[row]; }
    FieldType pack_key_holder(FieldType key, Arena&) const { return key; }

    std::span<FieldType> get_keys() const { return std::span<FieldType>((FieldType*)vec, size); }
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
    std::vector<StringRef> keys;

    HashMethodString(const ColumnRawPtrs& key_columns, const Sizes& /*key_sizes*/,
                     const HashMethodContextPtr&) {
        const IColumn& column = *key_columns[0];
        const ColumnString& column_string = assert_cast<const ColumnString&>(column);
        offsets = column_string.get_offsets().data();
        chars = column_string.get_chars().data();

        keys.resize(column_string.size());
        for (size_t row = 0; row < column_string.size(); row++) {
            keys[row] = StringRef(chars + offsets[row - 1], offsets[row] - offsets[row - 1]);
        }
    }

    auto get_key_holder(ssize_t row, [[maybe_unused]] Arena& pool) const {
        if constexpr (place_string_to_arena) {
            return ArenaKeyHolder {keys[row], pool};
        } else {
            return keys[row];
        }
    }

    const std::vector<StringRef>& get_keys() const { return keys; }

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache>;
};

/** Hash by concatenating serialized key values.
  * The serialized value differs in that it uniquely allows to deserialize it, having only the position with which it starts.
  * That is, for example, for strings, it contains first the serialized length of the string, and then the bytes.
  * Therefore, when aggregating by several strings, there is no ambiguity.
  */
template <typename Value, typename Mapped, bool keys_pre_serialized = false>
struct HashMethodSerialized
        : public columns_hashing_impl::HashMethodBase<
                  HashMethodSerialized<Value, Mapped, keys_pre_serialized>, Value, Mapped, false> {
    using Self = HashMethodSerialized<Value, Mapped, keys_pre_serialized>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;
    using KeyHolderType =
            std::conditional_t<keys_pre_serialized, ArenaKeyHolder, SerializedKeyHolder>;

    ColumnRawPtrs key_columns;
    size_t keys_size;
    StringRef* keys;

    HashMethodSerialized(const ColumnRawPtrs& key_columns_, const Sizes& /*key_sizes*/,
                         const HashMethodContextPtr&)
            : key_columns(key_columns_), keys_size(key_columns_.size()) {}

    void set_serialized_keys(StringRef* keys_) { keys = keys_; }

    ALWAYS_INLINE KeyHolderType get_key_holder(size_t row, Arena& pool) const {
        if constexpr (keys_pre_serialized) {
            return KeyHolderType {keys[row], pool};
        } else {
            return KeyHolderType {
                    serialize_keys_to_pool_contiguous(row, keys_size, key_columns, pool), pool};
        }
    }

    KeyHolderType pack_key_holder(StringRef key, Arena& pool) const {
        return KeyHolderType {key, pool};
    }

    std::span<StringRef> get_keys() const {
        return std::span<StringRef>(keys, key_columns[0]->size());
    }

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;
};

template <typename HashMethod>
struct IsPreSerializedKeysHashMethodTraits {
    constexpr static bool value = false;
};

template <typename Value, typename Mapped>
struct IsPreSerializedKeysHashMethodTraits<HashMethodSerialized<Value, Mapped, true>> {
    constexpr static bool value = true;
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

/// For the case when all keys are of fixed length, and they fit in N (for example, 128) bits.
template <typename Value, typename Key, typename Mapped, bool has_nullable_keys_ = false,
          bool use_cache = true>
struct HashMethodKeysFixed
        : private columns_hashing_impl::BaseStateKeysFixed<Key, has_nullable_keys_>,
          public columns_hashing_impl::HashMethodBase<
                  HashMethodKeysFixed<Value, Key, Mapped, has_nullable_keys_, use_cache>, Value,
                  Mapped, use_cache> {
    using Self = HashMethodKeysFixed<Value, Key, Mapped, has_nullable_keys_, use_cache>;
    using BaseHashed = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache>;
    using Base = columns_hashing_impl::BaseStateKeysFixed<Key, has_nullable_keys_>;

    const Sizes& key_sizes;
    size_t keys_size;
    std::vector<Key> keys;

    HashMethodKeysFixed(const ColumnRawPtrs& key_columns, const Sizes& key_sizes_,
                        const HashMethodContextPtr&)
            : Base(key_columns), key_sizes(key_sizes_), keys_size(key_columns.size()) {
        keys = pack_fixeds<Key>(key_columns[0]->size(), Base::get_actual_columns(), key_sizes,
                                Base::get_nullmap_columns());
    }

    ALWAYS_INLINE Key get_key_holder(size_t row, Arena&) const { return keys[row]; }

    Key pack_key_holder(Key key, Arena& pool) const { return key; }

    const std::vector<Key>& get_keys() const { return keys; }
};

template <typename SingleColumnMethod, typename Mapped, bool use_cache>
struct HashMethodSingleLowNullableColumn : public SingleColumnMethod {
    using Base = SingleColumnMethod;

    static constexpr bool has_mapped = !std::is_same<Mapped, void>::value;
    using EmplaceResult = columns_hashing_impl::EmplaceResultImpl<Mapped>;
    using FindResult = columns_hashing_impl::FindResultImpl<Mapped>;

    static HashMethodContextPtr createContext(const HashMethodContext::Settings& settings) {
        return nullptr;
    }

    const ColumnNullable* key_column;

    static const ColumnRawPtrs get_nested_column(const IColumn* col) {
        auto* nullable = check_and_get_column<ColumnNullable>(*col);
        DCHECK(nullable != nullptr);
        const auto nested_col = nullable->get_nested_column_ptr().get();
        return {nested_col};
    }

    HashMethodSingleLowNullableColumn(const ColumnRawPtrs& key_columns_nullable,
                                      const Sizes& key_sizes, const HashMethodContextPtr& context)
            : Base(get_nested_column(key_columns_nullable[0]), key_sizes, context),
              key_column(assert_cast<const ColumnNullable*>(key_columns_nullable[0])) {}

    template <typename Data>
    ALWAYS_INLINE EmplaceResult emplace_key(Data& data, size_t row, Arena& pool) {
        if (key_column->is_null_at(row)) {
            bool has_null_key = data.has_null_key_data();
            data.has_null_key_data() = true;

            if constexpr (has_mapped)
                return EmplaceResult(data.get_null_key_data(), data.get_null_key_data(),
                                     !has_null_key);
            else
                return EmplaceResult(!has_null_key);
        }

        auto key_holder = Base::get_key_holder(row, pool);

        bool inserted = false;
        typename Data::LookupResult it;
        data.emplace(key_holder, it, inserted);

        if constexpr (has_mapped) {
            auto& mapped = *lookup_result_get_mapped(it);
            if (inserted) {
                new (&mapped) Mapped();
            }
            return EmplaceResult(mapped, mapped, inserted);
        } else {
            return EmplaceResult(inserted);
        }
    }

    template <typename Data, typename KeyHolder>
    ALWAYS_INLINE EmplaceResult emplace_with_key(Data& data, KeyHolder&& key, size_t row) {
        if (key_column->is_null_at(row)) {
            bool has_null_key = data.has_null_key_data();
            data.has_null_key_data() = true;

            if constexpr (has_mapped) {
                return EmplaceResult(data.get_null_key_data(), data.get_null_key_data(),
                                     !has_null_key);
            } else {
                return EmplaceResult(!has_null_key);
            }
        }

        bool inserted = false;
        typename Data::LookupResult it;
        data.emplace(key, it, inserted);

        if constexpr (has_mapped) {
            auto& mapped = *lookup_result_get_mapped(it);
            if (inserted) {
                new (&mapped) Mapped();
            }
            return EmplaceResult(mapped, mapped, inserted);
        } else {
            return EmplaceResult(inserted);
        }
    }

    template <typename Data, typename KeyHolder>
    EmplaceResult emplace_with_key(Data& data, KeyHolder&& key, size_t hash_value, size_t row) {
        if (key_column->is_null_at(row)) {
            bool has_null_key = data.has_null_key_data();
            data.has_null_key_data() = true;

            if constexpr (has_mapped) {
                return EmplaceResult(data.get_null_key_data(), data.get_null_key_data(),
                                     !has_null_key);
            } else {
                return EmplaceResult(!has_null_key);
            }
        }

        bool inserted = false;
        typename Data::LookupResult it;
        data.emplace(key, it, hash_value, inserted);

        if constexpr (has_mapped) {
            auto& mapped = *lookup_result_get_mapped(it);
            if (inserted) {
                new (&mapped) Mapped();
            }
            return EmplaceResult(mapped, mapped, inserted);
        } else {
            return EmplaceResult(inserted);
        }
    }

    template <typename Data, typename Func, typename CreatorForNull>
        requires has_mapped
    ALWAYS_INLINE Mapped& lazy_emplace_key(Data& data, size_t row, Arena& pool, Func&& f,
                                           CreatorForNull&& null_creator) {
        if (key_column->is_null_at(row)) {
            bool has_null_key = data.has_null_key_data();
            data.has_null_key_data() = true;
            if (!has_null_key) std::forward<CreatorForNull>(null_creator)(data.get_null_key_data());
            return data.get_null_key_data();
        }
        auto key_holder = Base::get_key_holder(row, pool);
        typename Data::LookupResult it;
        data.lazy_emplace(key_holder, it, std::forward<Func>(f));
        return *lookup_result_get_mapped(it);
    }

    template <typename Data, typename Func, typename CreatorForNull>
        requires has_mapped
    ALWAYS_INLINE Mapped& lazy_emplace_key(Data& data, size_t row, Arena& pool, size_t hash_value,
                                           Func&& f, CreatorForNull&& null_creator) {
        if (key_column->is_null_at(row)) {
            bool has_null_key = data.has_null_key_data();
            data.has_null_key_data() = true;
            if (!has_null_key) std::forward<CreatorForNull>(null_creator)(data.get_null_key_data());
            return data.get_null_key_data();
        }
        auto key_holder = Base::get_key_holder(row, pool);
        typename Data::LookupResult it;
        data.lazy_emplace(key_holder, it, hash_value, std::forward<Func>(f));
        return *lookup_result_get_mapped(it);
    }

    template <typename Data>
    ALWAYS_INLINE FindResult find_key(Data& data, size_t row, Arena& pool) {
        if (key_column->is_null_at(row)) {
            bool has_null_key = data.has_null_key_data();
            if constexpr (has_mapped)
                return FindResult(&data.get_null_key_data(), has_null_key);
            else
                return FindResult(has_null_key);
        }
        auto key_holder = Base::get_key_holder(row, pool);
        auto key = key_holder_get_key(key_holder);
        auto it = data.find(key);
        if constexpr (has_mapped)
            return FindResult(it ? lookup_result_get_mapped(it) : nullptr, it != nullptr);
        else
            return FindResult(it != nullptr);
    }
};

template <typename HashMethod>
struct IsSingleNullableColumnMethod {
    static constexpr bool value = false;
};

template <typename SingleColumnMethod, typename Mapped, bool use_cache>
struct IsSingleNullableColumnMethod<
        HashMethodSingleLowNullableColumn<SingleColumnMethod, Mapped, use_cache>> {
    static constexpr bool value = true;
};

} // namespace ColumnsHashing
} // namespace doris::vectorized
