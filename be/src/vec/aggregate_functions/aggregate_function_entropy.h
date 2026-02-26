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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionEntropy.cpp
// and modified by Doris

#pragma once

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_decimal.h"
#include "vec/common/assert_cast.h"
#include "vec/common/hash_table/hash.h"
#include "vec/common/hash_table/phmap_fwd_decl.h"
#include "vec/common/string_ref.h"
#include "vec/common/uint128.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

class Arena;
class BufferReadable;
class BufferWritable;
template <PrimitiveType>
class ColumnVector;

/** Calculates Shannon Entropy, using HashMap and computing empirical distribution function.
  * Entropy is measured in bits (base-2 logarithm is used).
  */
template <typename Value, typename Hash = HashCRC32<Value>>
struct AggregateFunctionEntropyData {
    using Container = flat_hash_map<Value, uint64_t, Hash>;
    using Self = AggregateFunctionEntropyData<Value, Hash>;
    Container frequency_map;
    uint64_t total_count = 0;

    void clear() {
        frequency_map.clear();
        total_count = 0;
    }

    void add(const Value& elem) {
        ++frequency_map[elem];
        ++total_count;
    }

    void merge(const Self& rhs) {
        frequency_map.reserve(frequency_map.size() + rhs.frequency_map.size());
        for (const auto& [elem, count] : rhs.frequency_map) {
            frequency_map[elem] += count;
        }
        total_count += rhs.total_count;
    }

    void write(BufferWritable& buf) const {
        buf.write_var_uint(frequency_map.size());
        for (const auto& [elem, count] : frequency_map) {
            buf.write_binary(elem);
            buf.write_binary(count);
        }
    }

    void read(BufferReadable& buf) {
        uint64_t new_size = 0;
        buf.read_var_uint(new_size);
        frequency_map.reserve(frequency_map.size() + new_size);

        Value elem;
        uint64_t count;
        for (size_t i = 0; i < new_size; ++i) {
            buf.read_binary(elem);
            buf.read_binary(count);
            frequency_map[elem] += count;
            total_count += count;
        }
    }

    Float64 get_result() const {
        if (total_count == 0) {
            return 0;
        }
        Float64 entropy = 0;
        for (const auto& [_, count] : frequency_map) {
            Float64 p = static_cast<Float64>(count) / static_cast<Float64>(total_count);
            entropy -= p * std::log2(p);
        }
        return entropy;
    }

    static String get_name() { return "entropy"; }
};

template <PrimitiveType T>
struct AggregateFunctionEntropySingleNumericData
        : public AggregateFunctionEntropyData<typename PrimitiveTypeTraits<T>::CppType> {
    using Base = AggregateFunctionEntropyData<typename PrimitiveTypeTraits<T>::CppType>;

    void add(const IColumn** columns, size_t /* columns_num */, size_t row_num, Arena&) {
        const auto& vec = assert_cast<const typename PrimitiveTypeTraits<T>::ColumnType&,
                                      TypeCheckOnRelease::DISABLE>(*columns[0])
                                  .get_data();
        Base::add(vec[row_num]);
    }
};

struct AggregateFunctionEntropySingleStringData
        : public AggregateFunctionEntropyData<UInt128, UInt128TrivialHash> {
    using Base = AggregateFunctionEntropyData<UInt128, UInt128TrivialHash>;

    void add(const IColumn** columns, size_t /* columns_num */, size_t row_num, Arena&) {
        auto key = columns[0]->get_data_at(row_num);
        auto hash_value = XXH_INLINE_XXH128(key.data, key.size, 0);
        Base::add(UInt128 {hash_value.high64, hash_value.low64});
    }
};

struct AggregateFunctionEntropyGenericData
        : public AggregateFunctionEntropyData<UInt128, UInt128TrivialHash> {
    using Base = AggregateFunctionEntropyData<UInt128, UInt128TrivialHash>;

    void add(const IColumn** columns, size_t columns_num, size_t row_num, Arena& arena) {
        const char* begin = nullptr;
        StringRef key(begin, 0);
        for (size_t i = 0; i < columns_num; ++i) {
            auto cur_ref = columns[i]->serialize_value_into_arena(row_num, arena, begin);
            key.data = cur_ref.data - key.size;
            key.size += cur_ref.size;
        }
        auto hash_value = XXH_INLINE_XXH128(key.data, key.size, 0);
        Base::add(UInt128 {hash_value.high64, hash_value.low64});
    }
};

template <typename Data>
class AggregateFunctionEntropy final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionEntropy<Data>>,
          VarargsExpression,
          NullableAggregateFunction {
private:
    size_t arguments_num;

public:
    AggregateFunctionEntropy(const DataTypes& arguments)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionEntropy<Data>>(arguments),
              arguments_num(arguments.size()) {}

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena& arena) const override {
        this->data(place).add(columns, arguments_num, row_num, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena&) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        auto& column = assert_cast<ColumnFloat64&>(to);
        column.get_data().push_back(this->data(place).get_result());
    }

    void reset(AggregateDataPtr place) const override { this->data(place).clear(); }

    String get_name() const override { return Data::get_name(); }

    DataTypePtr get_return_type() const override {
        return std::make_shared<DataTypeNumber<TYPE_DOUBLE>>();
    }
};

} // namespace doris::vectorized

#include "common/compile_check_end.h"
