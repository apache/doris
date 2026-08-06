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

#include <memory>
#include <vector>

#include "core/block/column_with_type_and_name.h"
#include "core/column/column_const.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"

namespace doris {

namespace {

AggregateFunctionPtr create_percentile_reservoir_function(bool is_window = false) {
    return AggregateFunctionSimpleFactory::instance().get(
            "percentile_reservoir",
            {std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()},
            std::make_shared<DataTypeFloat64>(), false, BeExecVersionManager::get_newest_version(),
            {.is_window_function = is_window, .column_names = {}});
}

ColumnWithTypeAndName create_value_block(const std::vector<double>& values) {
    auto value_column = ColumnFloat64::create();
    for (double value : values) {
        value_column->insert_value(value);
    }
    return {std::move(value_column), std::make_shared<DataTypeFloat64>(), "value"};
}

double read_result(AggregateFunctionPtr fn, AggregateDataPtr place) {
    auto result_column = ColumnFloat64::create();
    fn->insert_result_into(place, *result_column);
    return result_column->get_element(0);
}

} // namespace

TEST(AggregateFunctionPercentileReservoirTest, optimized_single_place_paths) {
    auto fn = create_percentile_reservoir_function();
    ASSERT_TRUE(fn != nullptr);

    std::vector<ColumnWithTypeAndName> arguments;
    arguments.emplace_back(create_value_block({1.0, 2.0, 3.0, 4.0}));
    arguments.emplace_back(create_value_block({0.5, 0.5, 0.5, 0.5}));

    Arena arena;
    std::unique_ptr<char[]> place_mem(new char[fn->size_of_data()]);
    AggregateDataPtr place = place_mem.get();
    fn->create(place);

    const IColumn* columns[] = {arguments[0].column.get(), arguments[1].column.get()};

    fn->add_batch_single_place(4, place, columns, arena);
    EXPECT_DOUBLE_EQ(read_result(fn, place), 2.5);

    fn->reset(place);

    UInt8 use_null_result = false;
    UInt8 could_use_previous_result = false;
    fn->add_range_single_place(0, 4, 1, 3, place, columns, arena, &use_null_result,
                               &could_use_previous_result);
    EXPECT_FALSE(use_null_result);
    EXPECT_TRUE(could_use_previous_result);
    EXPECT_DOUBLE_EQ(read_result(fn, place), 2.5);

    fn->reset(place);
    use_null_result = false;
    could_use_previous_result = false;
    fn->add_range_single_place(0, 4, 4, 4, place, columns, arena, &use_null_result,
                               &could_use_previous_result);
    EXPECT_TRUE(use_null_result);
    EXPECT_FALSE(could_use_previous_result);

    fn->destroy(place);
}

} // namespace doris
