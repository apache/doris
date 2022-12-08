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

#include <parallel_hashmap/phmap.h>

#include <type_traits>

#include "gutil/hash/city.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_decimal.h"
#include "vec/common/aggregation_common.h"
#include "vec/common/assert_cast.h"
#include "vec/common/bit_cast.h"
#include "vec/common/hash_table/hash_set.h"
#include "vec/common/typeid_cast.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

// Here is an empirical value.
static constexpr size_t HASH_MAP_PREFETCH_DIST = 16;

/// uniqExact

template <typename T>
struct AggregateFunctionUniqExactData {
    static constexpr bool is_string_key = std::is_same_v<T, String>;
    using Key = std::conditional_t<is_string_key, UInt128, T>;
    using Hash = std::conditional_t<is_string_key, UInt128TrivialHash, HashCRC32<Key>>;

    using Set = phmap::flat_hash_set<Key, Hash>;

    static UInt128 ALWAYS_INLINE get_key(const StringRef& value) {
        UInt128 key;
        SipHash hash;
        hash.update(value.data, value.size);
        hash.get128(key.low, key.high);
        return key;
    }

    Set set;

    static String get_name() { return "uniqExact"; }
};

namespace detail {

/** The structure for the delegation work to add one element to the `uniq` aggregate functions.
  * Used for partial specialization to add strings.
  */
template <typename T, typename Data>
struct OneAdder {
    static void ALWAYS_INLINE add(Data& data, const IColumn& column, size_t row_num) {
        if constexpr (std::is_same_v<T, String>) {
            StringRef value = column.get_data_at(row_num);
            data.set.insert(Data::get_key(value));
        } else if constexpr (IsDecimalNumber<T>) {
            data.set.insert(assert_cast<const ColumnDecimal<T>&>(column).get_data()[row_num]);
        } else {
            data.set.insert(assert_cast<const ColumnVector<T>&>(column).get_data()[row_num]);
        }
    }
};

} // namespace detail

/// Calculates the number of different values approximately or exactly.
template <typename T, typename Data>
class AggregateFunctionUniq final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionUniq<T, Data>> {
public:
    using KeyType = std::conditional_t<std::is_same_v<T, String>, UInt128, T>;
    AggregateFunctionUniq(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionUniq<T, Data>>(argument_types_,
                                                                                 {}) {}

    String get_name() const override { return Data::get_name(); }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        detail::OneAdder<T, Data>::add(this->data(place), *columns[0], row_num);
    }

    static ALWAYS_INLINE const KeyType* get_keys(std::vector<KeyType>& keys_container,
                                                 const IColumn& column, size_t batch_size) {
        if constexpr (std::is_same_v<T, String>) {
            keys_container.resize(batch_size);
            for (size_t i = 0; i != batch_size; ++i) {
                StringRef value = column.get_data_at(i);
                keys_container[i] = Data::get_key(value);
            }
            return keys_container.data();
        } else {
            using ColumnType =
                    std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
            return assert_cast<const ColumnType&>(column).get_data().data();
        }
    }

    void add_batch(size_t batch_size, AggregateDataPtr* places, size_t place_offset,
                   const IColumn** columns, Arena* arena, bool /*agg_many*/) const override {
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
               Arena*) const override {
        auto& rhs_set = this->data(rhs).set;
        if (rhs_set.size() == 0) return;

        auto& set = this->data(place).set;
        set.rehash(set.size() + rhs_set.size());

        for (auto elem : rhs_set) {
            set.insert(elem);
        }
    }

    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
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
        write_var_uint(set.size(), buf);
        for (const auto& elem : set) {
            write_pod_binary(elem, buf);
        }
    }

    void deserialize_and_merge(AggregateDataPtr __restrict place, BufferReadable& buf,
                               Arena* arena) const override {
        auto& set = this->data(place).set;
        UInt64 size;
        read_var_uint(size, buf);

        set.rehash(size + set.size());

        for (size_t i = 0; i < size; ++i) {
            KeyType ref;
            read_pod_binary(ref, buf);
            set.insert(ref);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena* arena) const override {
        deserialize_and_merge(place, buf, arena);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        assert_cast<ColumnInt64&>(to).get_data().push_back(this->data(place).set.size());
    }
};

} // namespace doris::vectorized
