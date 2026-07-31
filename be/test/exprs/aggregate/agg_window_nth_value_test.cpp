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

#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"

namespace doris {

void register_aggregate_function_window_lead_lag_first_last(
        AggregateFunctionSimpleFactory& factory);

TEST(AggregateWindowNthValueTest, UpperBoundedLowerUnboundedFrame) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_window_lead_lag_first_last(factory);

    DataTypes argument_types = {std::make_shared<DataTypeString>(),
                                std::make_shared<DataTypeInt64>()};
    auto function = factory.get("nth_value", argument_types, nullptr, true, -1,
                                {.is_window_function = true, .column_names = {}});
    ASSERT_NE(function, nullptr);

    auto value_column = ColumnString::create();
    value_column->insert_data("C", 1);
    value_column->insert_data("B", 1);
    value_column->insert_data("A", 1);

    auto offset_column = ColumnInt64::create();
    offset_column->insert_value(-2);

    const IColumn* columns[] = {value_column.get(), offset_column.get()};

    Arena arena;
    auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(function->size_of_data()));
    function->create(place);

    auto result_column = ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
    UInt8 use_null_result = false;
    UInt8 could_use_previous_result = false;

    function->add_range_single_place(0, 3, 0, 1, place, columns, arena, &use_null_result,
                                     &could_use_previous_result);
    function->insert_result_into(place, *result_column);

    function->add_range_single_place(0, 3, 1, 2, place, columns, arena, &use_null_result,
                                     &could_use_previous_result);
    function->insert_result_into(place, *result_column);

    function->add_range_single_place(0, 3, 2, 3, place, columns, arena, &use_null_result,
                                     &could_use_previous_result);
    function->insert_result_into(place, *result_column);

    ASSERT_EQ(result_column->size(), 3);
    EXPECT_TRUE(result_column->is_null_at(0));
    EXPECT_EQ(result_column->get_data_at(1).to_string(), "C");
    EXPECT_EQ(result_column->get_data_at(2).to_string(), "B");

    function->destroy(place);
}

} // namespace doris
