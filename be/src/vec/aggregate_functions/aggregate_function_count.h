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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionCount.h
// and modified by Doris

#pragma once

#include <array>

#include "common/logging.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

struct AggregateFunctionCountData {
    UInt64 count = 0;
};

/// Simply count number of calls.
class AggregateFunctionCount final
        : public IAggregateFunctionDataHelper<AggregateFunctionCountData, AggregateFunctionCount> {
public:
    AggregateFunctionCount(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper(argument_types_, {}) {}

    String get_name() const override { return "count"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn**, size_t, Arena*) const override {
        ++data(place).count;
    }

    void reset(AggregateDataPtr place) const override {
        AggregateFunctionCount::data(place).count = 0;
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        data(place).count += data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        write_var_uint(data(place).count, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        read_var_uint(data(place).count, buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        assert_cast<ColumnInt64&>(to).get_data().push_back(data(place).count);
    }

    void deserialize_from_column(AggregateDataPtr places, const IColumn& column, Arena* arena,
                                 size_t num_rows) const override {
        auto data = assert_cast<const ColumnUInt64&>(column).get_data().data();
        auto* dst_data = reinterpret_cast<Data*>(places);
        for (size_t i = 0; i != num_rows; ++i) {
            dst_data[i].count = data[i];
        }
    }

    void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                             MutableColumnPtr& dst, const size_t num_rows) const override {
        auto& col = assert_cast<ColumnUInt64&>(*dst);
        col.resize(num_rows);
        auto* data = col.get_data().data();
        for (size_t i = 0; i != num_rows; ++i) {
            data[i] = this->data(places[i] + offset).count;
        }
    }

    void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                           const size_t num_rows, Arena* arena) const override {
        auto& col = assert_cast<ColumnUInt64&>(*dst);
        col.resize(num_rows);
        col.get_data().assign(num_rows, static_cast<UInt64>(1UL));
    }

    void deserialize_and_merge_from_column(AggregateDataPtr __restrict place, const IColumn& column,
                                           Arena* arena) const override {
        auto data = assert_cast<const ColumnUInt64&>(column).get_data().data();
        const size_t num_rows = column.size();
        for (size_t i = 0; i != num_rows; ++i) {
            this->data(place).count += data[i];
        }
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         MutableColumnPtr& dst) const override {
        auto& col = assert_cast<ColumnUInt64&>(*dst);
        col.resize(1);
        reinterpret_cast<Data*>(col.get_data().data())->count = this->data(place).count;
    }

    MutableColumnPtr create_serialize_column() const override {
        return ColumnVector<UInt64>::create();
    }

    DataTypePtr get_serialized_type() const override { return std::make_shared<DataTypeUInt64>(); }
};

/// Simply count number of not-NULL values.
class AggregateFunctionCountNotNullUnary final
        : public IAggregateFunctionDataHelper<AggregateFunctionCountData,
                                              AggregateFunctionCountNotNullUnary> {
public:
    AggregateFunctionCountNotNullUnary(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper(argument_types_, {}) {}

    String get_name() const override { return "count"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        data(place).count += !assert_cast<const ColumnNullable&>(*columns[0]).is_null_at(row_num);
    }

    void reset(AggregateDataPtr place) const override { data(place).count = 0; }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        data(place).count += data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        write_var_uint(data(place).count, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        read_var_uint(data(place).count, buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        if (to.is_nullable()) {
            auto& null_column = assert_cast<ColumnNullable&>(to);
            null_column.get_null_map_data().push_back(0);
            assert_cast<ColumnInt64&>(null_column.get_nested_column())
                    .get_data()
                    .push_back(data(place).count);
        } else {
            assert_cast<ColumnInt64&>(to).get_data().push_back(data(place).count);
        }
    }

    void deserialize_from_column(AggregateDataPtr places, const IColumn& column, Arena* arena,
                                 size_t num_rows) const override {
        auto data = assert_cast<const ColumnUInt64&>(column).get_data().data();
        auto* dst_data = reinterpret_cast<Data*>(places);
        for (size_t i = 0; i != num_rows; ++i) {
            dst_data[i].count = data[i];
        }
    }

    void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                             MutableColumnPtr& dst, const size_t num_rows) const override {
        auto& col = assert_cast<ColumnUInt64&>(*dst);
        col.resize(num_rows);
        auto* data = col.get_data().data();
        for (size_t i = 0; i != num_rows; ++i) {
            data[i] = this->data(places[i] + offset).count;
        }
    }

    void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                           const size_t num_rows, Arena* arena) const override {
        auto& col = assert_cast<ColumnUInt64&>(*dst);
        col.resize(num_rows);
        auto& data = col.get_data();
        const ColumnNullable& input_col = assert_cast<const ColumnNullable&>(*columns[0]);
        for (size_t i = 0; i < num_rows; i++) {
            data[i] = !input_col.is_null_at(i);
        }
    }

    void deserialize_and_merge_from_column(AggregateDataPtr __restrict place, const IColumn& column,
                                           Arena* arena) const override {
        auto data = assert_cast<const ColumnUInt64&>(column).get_data().data();
        const size_t num_rows = column.size();
        for (size_t i = 0; i != num_rows; ++i) {
            this->data(place).count += data[i];
        }
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         MutableColumnPtr& dst) const override {
        auto& col = assert_cast<ColumnUInt64&>(*dst);
        col.resize(1);
        reinterpret_cast<Data*>(col.get_data().data())->count = this->data(place).count;
    }

    MutableColumnPtr create_serialize_column() const override {
        return ColumnVector<UInt64>::create();
    }

    DataTypePtr get_serialized_type() const override { return std::make_shared<DataTypeUInt64>(); }
};

} // namespace doris::vectorized
