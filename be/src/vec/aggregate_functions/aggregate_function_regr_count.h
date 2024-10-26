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

#include <boost/iterator/iterator_facade.hpp>
#include <cmath>
#include <cstddef>
#include <memory>

#include "olap/olap_common.h"
#include "runtime/decimalv2_value.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

struct AggregateFunctionRegrCountData {
    UInt64 count = 0;
};

class AggregateFunctionRegrCount final
        : public IAggregateFunctionDataHelper<AggregateFunctionRegrCountData,
                                              AggregateFunctionRegrCount> {
public:
    AggregateFunctionRegrCount(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionRegrCount>(argument_types_) {}

    auto get_name() const -> String override { return "regr_count"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        bool first = true;
        if (columns[0]->is_nullable()) {
            first = !assert_cast<const ColumnNullable&>(*columns[0]).is_null_at(row_num);
        }

        bool second = true;
        if (columns[1]->is_nullable()) {
            second = !assert_cast<const ColumnNullable&>(*columns[1]).is_null_at(row_num);
        }
        data(place).count += first && second;
    }

    void reset(AggregateDataPtr __restrict place) const override { data(place).count = 0; }

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
};
} // namespace doris::vectorized
