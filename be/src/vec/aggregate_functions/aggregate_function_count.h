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

    void add(AggregateDataPtr place, const IColumn**, size_t, Arena*) const override {
        ++data(place).count;
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena*) const override {
        data(place).count += data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {
        write_var_uint(data(place).count, buf);
    }

    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena*) const override {
        read_var_uint(data(place).count, buf);
    }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        assert_cast<ColumnInt64&>(to).get_data().push_back(data(place).count);
    }

    const char* get_header_file_path() const override { return __FILE__; }
};

/// Simply count number of not-NULL values.
class AggregateFunctionCountNotNullUnary final
        : public IAggregateFunctionDataHelper<AggregateFunctionCountData,
                                              AggregateFunctionCountNotNullUnary> {
public:
    AggregateFunctionCountNotNullUnary(const DataTypePtr& argument, const Array& params)
            : IAggregateFunctionDataHelper<AggregateFunctionCountData,
                                           AggregateFunctionCountNotNullUnary>({argument}, params) {
        if (!argument->is_nullable()) {
            LOG(FATAL) << "Logical error: not Nullable data type passed to "
                          "AggregateFunctionCountNotNullUnary";
        }
    }

    AggregateFunctionCountNotNullUnary(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper(argument_types_, {}) {}

    String get_name() const override { return "count"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        data(place).count += !assert_cast<const ColumnNullable&>(*columns[0]).is_null_at(row_num);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena*) const override {
        data(place).count += data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {
        write_var_uint(data(place).count, buf);
    }

    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena*) const override {
        read_var_uint(data(place).count, buf);
    }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        assert_cast<ColumnInt64&>(to).get_data().push_back(data(place).count);
    }

    const char* get_header_file_path() const override { return __FILE__; }
};

} // namespace doris::vectorized
