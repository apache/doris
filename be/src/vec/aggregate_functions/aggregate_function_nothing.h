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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionNothing.h
// and modified by Doris

#pragma once

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

/** Aggregate function that takes arbitrary number of arbitrary arguments and does nothing.
  */
class AggregateFunctionNothing final : public IAggregateFunctionHelper<AggregateFunctionNothing> {
public:
    AggregateFunctionNothing(const DataTypes& arguments, const Array& params)
            : IAggregateFunctionHelper<AggregateFunctionNothing>(arguments, params) {}

    String get_name() const override { return "nothing"; }

    DataTypePtr get_return_type() const override {
        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());
    }

    void create(AggregateDataPtr) const override {}

    void destroy(AggregateDataPtr) const noexcept override {}

    bool has_trivial_destructor() const override { return true; }

    size_t size_of_data() const override { return 0; }

    size_t align_of_data() const override { return 1; }

    void add(AggregateDataPtr, const IColumn**, size_t, Arena*) const override {}

    void reset(AggregateDataPtr place) const override {}

    void merge(AggregateDataPtr, ConstAggregateDataPtr, Arena*) const override {}

    void serialize(ConstAggregateDataPtr, BufferWritable& buf) const override {}

    void deserialize(AggregateDataPtr, BufferReadable& buf, Arena*) const override {}

    void insert_result_into(ConstAggregateDataPtr, IColumn& to) const override {
        to.insert_default();
    }
};

} // namespace doris::vectorized
