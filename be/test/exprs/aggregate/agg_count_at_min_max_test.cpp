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

#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "core/column/column.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "exprs/aggregate/single_value_data.h"

namespace doris {

void register_aggregate_function_count_at_min_max(AggregateFunctionSimpleFactory& factory);

namespace {

AggregateFunctionPtr create_function(AggregateFunctionSimpleFactory& factory,
                                     const std::string& name, const DataTypes& argument_types) {
    auto function = factory.get(name, argument_types, nullptr, false, -1);
    EXPECT_NE(function, nullptr);
    return function;
}

void aggregate_int32(IAggregateFunction* function, Arena& arena, const IColumn& column,
                     int64_t expected) {
    std::unique_ptr<char[]> memory(new char[function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    function->create(place);
    const IColumn* columns[1] = {&column};
    for (size_t row = 0; row < column.size(); ++row) {
        function->add(place, columns, row, arena);
    }
    ColumnInt64 result;
    function->insert_result_into(place, result);
    EXPECT_EQ(expected, result.get_element(0));
    function->destroy(place);
}

} // namespace

TEST(CountAtMinMaxTest, Int32) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_count_at_min_max(factory);
    DataTypes data_types = {std::make_shared<DataTypeInt32>()};

    auto column = ColumnInt32::create();
    for (int value : {1, 2, 1, 3, 1, 2}) {
        column->insert_value(value);
    }

    Arena arena;
    auto count_at_min = create_function(factory, "count_at_min", data_types);
    aggregate_int32(count_at_min.get(), arena, *column, 3);

    auto count_at_max = create_function(factory, "count_at_max", data_types);
    aggregate_int32(count_at_max.get(), arena, *column, 1);
}

TEST(CountAtMinMaxTest, EmptyInput) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_count_at_min_max(factory);
    DataTypes data_types = {std::make_shared<DataTypeInt32>()};
    Arena arena;

    auto column = ColumnInt32::create();
    for (const auto& name : {"count_at_min", "count_at_max"}) {
        auto function = create_function(factory, name, data_types);
        aggregate_int32(function.get(), arena, *column, 0);
    }
}

TEST(CountAtMinMaxTest, Merge) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_count_at_min_max(factory);
    DataTypes data_types = {std::make_shared<DataTypeInt32>()};
    Arena arena;

    auto first = ColumnInt32::create();
    auto second = ColumnInt32::create();
    for (int value : {5, 1, 5}) {
        first->insert_value(value);
    }
    for (int value : {5, 1, 9}) {
        second->insert_value(value);
    }

    auto function = create_function(factory, "count_at_min", data_types);
    std::unique_ptr<char[]> memory(new char[function->size_of_data() * 2]);
    AggregateDataPtr lhs = memory.get();
    AggregateDataPtr rhs = memory.get() + function->size_of_data();
    function->create(lhs);
    function->create(rhs);

    const IColumn* first_columns[1] = {first.get()};
    const IColumn* second_columns[1] = {second.get()};
    for (size_t row = 0; row < first->size(); ++row) {
        function->add(lhs, first_columns, row, arena);
    }
    for (size_t row = 0; row < second->size(); ++row) {
        function->add(rhs, second_columns, row, arena);
    }
    function->merge(lhs, rhs, arena);

    ColumnInt64 result;
    function->insert_result_into(lhs, result);
    EXPECT_EQ(2, result.get_element(0));
    function->destroy(lhs);
    function->destroy(rhs);
}

TEST(CountAtMinMaxTest, String) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_count_at_min_max(factory);
    DataTypes data_types = {std::make_shared<DataTypeString>()};
    Arena arena;

    auto column = ColumnString::create();
    for (const auto& value : {"b", "a", "b", "a", "c", "a"}) {
        column->insert_data(value, strlen(value));
    }

    for (const auto& [name, expected] : {std::pair {"count_at_min", 3}, {"count_at_max", 1}}) {
        auto function = create_function(factory, name, data_types);
        std::unique_ptr<char[]> memory(new char[function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        function->create(place);
        const IColumn* columns[1] = {column.get()};
        for (size_t row = 0; row < column->size(); ++row) {
            function->add(place, columns, row, arena);
        }
        ColumnInt64 result;
        function->insert_result_into(place, result);
        EXPECT_EQ(expected, result.get_element(0));
        function->destroy(place);
    }
}

} // namespace doris
