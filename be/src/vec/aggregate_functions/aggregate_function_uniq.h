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

/// uniqExact

template <typename T>
struct AggregateFunctionUniqExactData {
    using Key = T;

    /// When creating, the hash table must be small.
    using Set = HashSet<Key, HashCRC32<Key>, HashTableGrower<4>,
                        HashTableAllocatorWithStackMemory<sizeof(Key) * (1 << 4)>>;

    Set set;

    static String get_name() { return "uniqExact"; }
};

/// For rows, we put the SipHash values (128 bits) into the hash table.
template <>
struct AggregateFunctionUniqExactData<String> {
    using Key = UInt128;

    /// When creating, the hash table must be small.
    using Set = HashSet<Key, UInt128TrivialHash, HashTableGrower<3>,
                        HashTableAllocatorWithStackMemory<sizeof(Key) * (1 << 3)>>;

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

            UInt128 key;
            SipHash hash;
            hash.update(value.data, value.size);
            hash.get128(key.low, key.high);

            data.set.insert(key);
        } else if constexpr (std::is_same_v<T, Decimal128>) {
            data.set.insert(
                    assert_cast<const ColumnDecimal<Decimal128>&>(column).get_data()[row_num]);
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
    AggregateFunctionUniq(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionUniq<T, Data>>(argument_types_,
                                                                                 {}) {}

    String get_name() const override { return Data::get_name(); }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        detail::OneAdder<T, Data>::add(this->data(place), *columns[0], row_num);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        this->data(place).set.merge(this->data(rhs).set);
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).set.write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).set.read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        assert_cast<ColumnInt64&>(to).get_data().push_back(this->data(place).set.size());
    }
};

} // namespace doris::vectorized
