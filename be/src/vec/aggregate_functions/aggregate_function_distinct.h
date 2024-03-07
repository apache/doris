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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionDistinct.h
// and modified by Doris

#pragma once

#include <assert.h>
#include <glog/logging.h>
#include <stddef.h>

#include <algorithm>
#include <memory>
#include <new>
#include <string>
#include <vector>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/common/assert_cast.h"
#include "vec/common/hash_table/hash_set.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/io/io_helper.h"
#include "vec/io/var_int.h"

namespace doris {
namespace vectorized {
class Arena;
class BufferReadable;
class BufferWritable;
template <typename>
class ColumnVector;
} // namespace vectorized
} // namespace doris
template <typename, typename>
struct DefaultHash;

namespace doris::vectorized {

template <typename T>
struct AggregateFunctionDistinctSingleNumericData {
    /// When creating, the hash table must be small.
    using Set = HashSetWithStackMemory<T, DefaultHash<T>, 4>;
    using Self = AggregateFunctionDistinctSingleNumericData<T>;
    Set set;

    void add(const IColumn** columns, size_t /* columns_num */, size_t row_num, Arena*) {
        const auto& vec = assert_cast<const ColumnVector<T>&>(*columns[0]).get_data();
        set.insert(vec[row_num]);
    }

    void merge(const Self& rhs, Arena*) { set.merge(rhs.set); }

    void serialize(BufferWritable& buf) const { set.write(buf); }

    void deserialize(BufferReadable& buf, Arena*) { set.read(buf); }

    MutableColumns get_arguments(const DataTypes& argument_types) const {
        MutableColumns argument_columns;
        argument_columns.emplace_back(argument_types[0]->create_column());
        for (const auto& elem : set) {
            argument_columns[0]->insert(elem.get_value());
        }

        return argument_columns;
    }
};

struct AggregateFunctionDistinctGenericData {
    /// When creating, the hash table must be small.
    using Set = HashSetWithStackMemory<StringRef, StringRefHash, 4>;
    using Self = AggregateFunctionDistinctGenericData;
    Set set;

    void merge(const Self& rhs, Arena* arena) {
        Set::LookupResult it;
        bool inserted;
        for (const auto& elem : rhs.set) {
            StringRef key = elem.get_value();
            key.data = arena->insert(key.data, key.size);
            set.emplace(key, it, inserted);
        }
    }

    void serialize(BufferWritable& buf) const {
        write_var_uint(set.size(), buf);
        for (const auto& elem : set) {
            write_string_binary(elem.get_value(), buf);
        }
    }

    void deserialize(BufferReadable& buf, Arena* arena) {
        UInt64 size;
        read_var_uint(size, buf);

        StringRef ref;
        for (size_t i = 0; i < size; ++i) {
            read_string_binary(ref, buf);
            set.insert(ref);
        }
    }
};

template <bool is_plain_column>
struct AggregateFunctionDistinctSingleGenericData : public AggregateFunctionDistinctGenericData {
    void add(const IColumn** columns, size_t /* columns_num */, size_t row_num, Arena* arena) {
        Set::LookupResult it;
        bool inserted;
        auto key = columns[0]->get_data_at(row_num);
        key.data = arena->insert(key.data, key.size);
        set.emplace(key, it, inserted);
    }

    MutableColumns get_arguments(const DataTypes& argument_types) const {
        MutableColumns argument_columns;
        argument_columns.emplace_back(argument_types[0]->create_column());
        for (const auto& elem : set) {
            argument_columns[0]->insert_data(elem.get_value().data, elem.get_value().size);
        }

        return argument_columns;
    }
};

struct AggregateFunctionDistinctMultipleGenericData : public AggregateFunctionDistinctGenericData {
    void add(const IColumn** columns, size_t columns_num, size_t row_num, Arena* arena) {
        const char* begin = nullptr;
        StringRef value(begin, 0);
        for (size_t i = 0; i < columns_num; ++i) {
            auto cur_ref = columns[i]->serialize_value_into_arena(row_num, *arena, begin);
            value.data = cur_ref.data - value.size;
            value.size += cur_ref.size;
        }

        Set::LookupResult it;
        bool inserted;
        value.data = arena->insert(value.data, value.size);
        set.emplace(value, it, inserted);
    }

    MutableColumns get_arguments(const DataTypes& argument_types) const {
        MutableColumns argument_columns(argument_types.size());
        for (size_t i = 0; i < argument_types.size(); ++i) {
            argument_columns[i] = argument_types[i]->create_column();
        }

        for (const auto& elem : set) {
            const char* begin = elem.get_value().data;
            for (auto& column : argument_columns) {
                begin = column->deserialize_and_insert_from_arena(begin);
            }
        }

        return argument_columns;
    }
};

/** Adaptor for aggregate functions.
  * Adding -Distinct suffix to aggregate function
**/
template <typename Data>
class AggregateFunctionDistinct
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionDistinct<Data>> {
private:
    size_t prefix_size;
    AggregateFunctionPtr nested_func;
    size_t arguments_num;

    AggregateDataPtr get_nested_place(AggregateDataPtr __restrict place) const noexcept {
        return place + prefix_size;
    }

    ConstAggregateDataPtr get_nested_place(ConstAggregateDataPtr __restrict place) const noexcept {
        return place + prefix_size;
    }

public:
    AggregateFunctionDistinct(AggregateFunctionPtr nested_func_, const DataTypes& arguments)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionDistinct>(arguments),
              nested_func(nested_func_),
              arguments_num(arguments.size()) {
        size_t nested_size = nested_func->align_of_data();
        CHECK_GT(nested_size, 0);
        prefix_size = (sizeof(Data) + nested_size - 1) / nested_size * nested_size;
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena* arena) const override {
        this->data(place).add(columns, arguments_num, row_num, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena* arena) const override {
        this->data(place).merge(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena* arena) const override {
        this->data(place).deserialize(buf, arena);
    }

    void insert_result_into(ConstAggregateDataPtr targetplace, IColumn& to) const override {
        auto place = const_cast<AggregateDataPtr>(targetplace);
        auto arguments = this->data(place).get_arguments(this->argument_types);
        ColumnRawPtrs arguments_raw(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i) {
            arguments_raw[i] = arguments[i].get();
        }

        assert(!arguments.empty());
        // nested_func->add_batch_single_place(arguments[0]->size(), get_nested_place(place), arguments_raw.data(), arena);
        // nested_func->insert_result_into(get_nested_place(place), to, arena);

        nested_func->add_batch_single_place(arguments[0]->size(), get_nested_place(place),
                                            arguments_raw.data(), nullptr);
        nested_func->insert_result_into(get_nested_place(place), to);
    }

    size_t size_of_data() const override { return prefix_size + nested_func->size_of_data(); }

    size_t align_of_data() const override { return nested_func->align_of_data(); }

    void create(AggregateDataPtr __restrict place) const override {
        new (place) Data;
        SAFE_CREATE(nested_func->create(get_nested_place(place)), this->data(place).~Data());
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override {
        this->data(place).~Data();
        nested_func->destroy(get_nested_place(place));
    }

    String get_name() const override { return nested_func->get_name() + "Distinct"; }

    DataTypePtr get_return_type() const override { return nested_func->get_return_type(); }

    bool allocates_memory_in_arena() const override { return true; }
};

} // namespace doris::vectorized
