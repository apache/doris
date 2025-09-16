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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionUniq.h
// and modified by Doris

#pragma once

#include <stddef.h>

#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <type_traits>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/hash_table/hash.h"
#include "vec/common/hash_table/phmap_fwd_decl.h"
#include "vec/common/string_ref.h"
#include "vec/common/uint128.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"
#include "vec/io/var_int.h"

template <typename T>
struct HashCRC32;

namespace doris::vectorized {
#include "common/compile_check_begin.h"

class Arena;
class BufferReadable;
class BufferWritable;
template <PrimitiveType T>
class ColumnDecimal;
/// uniqExact

template <PrimitiveType T>
struct AggregateFunctionUniqExactData {
    static constexpr bool is_string_key = is_string_type(T);
    using Key = std::conditional_t<
            is_string_key, UInt128,
            std::conditional_t<T == TYPE_ARRAY, UInt64,
                               std::conditional_t<T == TYPE_BOOLEAN, UInt8,
                                                  typename PrimitiveTypeTraits<T>::CppNativeType>>>;
    using Hash = HashCRC32<Key>;

    using Set = flat_hash_set<Key, Hash>;

    static UInt128 ALWAYS_INLINE get_key(const StringRef& value) {
        auto hash_value = XXH_INLINE_XXH128(value.data, value.size, 0);
        return UInt128 {hash_value.high64, hash_value.low64};
    }
    static UInt64 ALWAYS_INLINE get_key(const IColumn& column, size_t row_num) {
        UInt64 hash_value = 0;
        column.update_xxHash_with_value(row_num, row_num + 1, hash_value, nullptr);
        return hash_value;
    }

    Set set;

    static String get_name() { return "multi_distinct"; }

    void reset() { set.clear(); }
};

namespace detail {

/** The structure for the delegation work to add one element to the `uniq` aggregate functions.
  * Used for partial specialization to add strings.
  */
template <PrimitiveType T, typename Data>
struct OneAdder {
    static void ALWAYS_INLINE add(Data& data, const IColumn& column, size_t row_num) {
        if constexpr (is_string_type(T)) {
            StringRef value = column.get_data_at(row_num);
            data.set.insert(Data::get_key(value));
        } else if constexpr (T == TYPE_ARRAY) {
            data.set.insert(Data::get_key(column, row_num));
        } else if constexpr (is_decimal(T)) {
            data.set.insert(assert_cast<const typename PrimitiveTypeTraits<T>::ColumnType&,
                                        TypeCheckOnRelease::DISABLE>(column)
                                    .get_data()[row_num]
                                    .value);
        } else {
            data.set.insert(assert_cast<const typename PrimitiveTypeTraits<T>::ColumnType&,
                                        TypeCheckOnRelease::DISABLE>(column)
                                    .get_data()[row_num]);
        }
    }
};

} // namespace detail

/// Calculates the number of different values approximately or exactly.
template <PrimitiveType T, typename Data>
class AggregateFunctionUniq final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionUniq<T, Data>>,
          VarargsExpression,
          NotNullableAggregateFunction {
public:
    using KeyType =
            std::conditional_t<is_string_type(T), UInt128,
                               std::conditional_t<T == TYPE_ARRAY, UInt64,
                                                  typename PrimitiveTypeTraits<T>::ColumnItemType>>;
    AggregateFunctionUniq(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionUniq<T, Data>>(argument_types_) {}

    String get_name() const override { return Data::get_name(); }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        detail::OneAdder<T, Data>::add(this->data(place), *columns[0], row_num);
    }

    static ALWAYS_INLINE const KeyType* get_keys(std::vector<KeyType>& keys_container,
                                                 const IColumn& column, size_t batch_size) {
        if constexpr (is_string_type(T)) {
            keys_container.resize(batch_size);
            for (size_t i = 0; i != batch_size; ++i) {
                StringRef value = column.get_data_at(i);
                keys_container[i] = Data::get_key(value);
            }
            return keys_container.data();
        } else if constexpr (T == TYPE_ARRAY) {
            keys_container.resize(batch_size);
            for (size_t i = 0; i != batch_size; ++i) {
                keys_container[i] = Data::get_key(column, i);
            }
            return keys_container.data();
        } else {
            return assert_cast<const typename PrimitiveTypeTraits<T>::ColumnType&>(column)
                    .get_data()
                    .data();
        }
    }

    void add_batch(size_t batch_size, AggregateDataPtr* places, size_t place_offset,
                   const IColumn** columns, Arena&, bool /*agg_many*/) const override {
        std::vector<KeyType> keys_container;
        const KeyType* keys = get_keys(keys_container, *columns[0], batch_size);

        std::vector<typename Data::Set*> array_of_data_set(batch_size);

        for (size_t i = 0; i != batch_size; ++i) {
            array_of_data_set[i] = &(this->data(places[i] + place_offset).set);
        }

        for (size_t i = 0; i != batch_size; ++i) {
            if (i + HASH_MAP_PREFETCH_DIST < batch_size) {
                array_of_data_set[i + HASH_MAP_PREFETCH_DIST]->prefetch(
                        keys[i + HASH_MAP_PREFETCH_DIST]);
            }

            array_of_data_set[i]->insert(keys[i]);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena&) const override {
        auto& rhs_set = this->data(rhs).set;
        if (rhs_set.size() == 0) return;

        auto& set = this->data(place).set;
        set.rehash(set.size() + rhs_set.size());

        for (auto elem : rhs_set) {
            set.insert(elem);
        }
    }

    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena&) const override {
        std::vector<KeyType> keys_container;
        const KeyType* keys = get_keys(keys_container, *columns[0], batch_size);
        auto& set = this->data(place).set;

        for (size_t i = 0; i != batch_size; ++i) {
            if (i + HASH_MAP_PREFETCH_DIST < batch_size) {
                set.prefetch(keys[i + HASH_MAP_PREFETCH_DIST]);
            }
            set.insert(keys[i]);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        auto& set = this->data(place).set;
        buf.write_var_uint(set.size());
        for (const auto& elem : set) {
            buf.write_binary(elem);
        }
    }

    void deserialize_and_merge(AggregateDataPtr __restrict place, AggregateDataPtr __restrict rhs,
                               BufferReadable& buf, Arena&) const override {
        auto& set = this->data(place).set;
        UInt64 size;
        buf.read_var_uint(size);

        set.rehash(size + set.size());

        for (size_t i = 0; i < size; ++i) {
            KeyType ref;
            buf.read_binary(ref);
            set.insert(ref);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        auto& set = this->data(place).set;
        UInt64 size;
        buf.read_var_uint(size);

        set.rehash(size + set.size());

        for (size_t i = 0; i < size; ++i) {
            KeyType ref;
            buf.read_binary(ref);
            set.insert(ref);
        }
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        assert_cast<ColumnInt64&>(to).get_data().push_back(this->data(place).set.size());
    }
};

} // namespace doris::vectorized

#include "common/compile_check_end.h"
