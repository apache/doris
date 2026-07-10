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

#include <cmath>
#include <memory>
#include <vector>

#include "common/exception.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/field.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"

namespace doris {

namespace {

AggregateFunctionPtr create_percentile_reservoir_function(const DataTypes& argument_types,
                                                          bool is_window = false,
                                                          bool use_null_v2 = false) {
    return AggregateFunctionSimpleFactory::instance().get(
            "percentile_reservoir", argument_types, std::make_shared<DataTypeFloat64>(), false,
            BeExecVersionManager::get_newest_version(),
            {.is_window_function = is_window,
             .enable_aggregate_function_null_v2 = use_null_v2,
             .column_names = {}});
}

AggregateFunctionPtr create_count_function(const DataTypes& argument_types, bool use_null_v2) {
    return AggregateFunctionSimpleFactory::instance().get(
            "count", argument_types, std::make_shared<DataTypeInt64>(), false,
            BeExecVersionManager::get_newest_version(),
            {.enable_aggregate_function_null_v2 = use_null_v2, .column_names = {}});
}

AggregateFunctionPtr create_sum_function(const DataTypes& argument_types, bool use_null_v2) {
    return AggregateFunctionSimpleFactory::instance().get(
            "sum", argument_types, std::make_shared<DataTypeInt64>(), false,
            BeExecVersionManager::get_newest_version(),
            {.enable_aggregate_function_null_v2 = use_null_v2, .column_names = {}});
}

AggregateFunctionPtr create_percentile_array_v2_function() {
    return AggregateFunctionSimpleFactory::instance().get(
            "percentile_array",
            {std::make_shared<DataTypeFloat64>(),
             std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat64>()))},
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat64>())),
            false, BeExecVersionManager::get_newest_version(),
            {.new_version_percentile = true, .column_names = {}});
}

ColumnWithTypeAndName create_value_block(const std::vector<double>& values) {
    auto value_column = ColumnFloat64::create();
    for (double value : values) {
        value_column->insert_value(value);
    }
    return {std::move(value_column), std::make_shared<DataTypeFloat64>(), "value"};
}

ColumnWithTypeAndName create_const_level(double value) {
    auto level_column = ColumnFloat64::create();
    level_column->insert_value(value);
    return {ColumnConst::create(std::move(level_column), 1), std::make_shared<DataTypeFloat64>(),
            "level"};
}

ColumnWithTypeAndName create_const_nullable_null_level(size_t size) {
    auto level_column = ColumnFloat64::create();
    level_column->insert_value(0.0);
    auto null_map = ColumnUInt8::create();
    null_map->insert_value(1);
    auto nullable = ColumnNullable::create(std::move(level_column), std::move(null_map));
    return {ColumnConst::create(std::move(nullable), size),
            make_nullable(std::make_shared<DataTypeFloat64>()), "nullable_level"};
}

ColumnWithTypeAndName create_nullable_int_column(const std::vector<int32_t>& values,
                                                 const std::vector<uint8_t>& null_map_values) {
    DCHECK_EQ(values.size(), null_map_values.size());
    auto data_column = ColumnInt32::create();
    auto null_map = ColumnUInt8::create();
    for (size_t i = 0; i < values.size(); ++i) {
        data_column->insert_value(values[i]);
        null_map->insert_value(null_map_values[i]);
    }
    return {ColumnNullable::create(std::move(data_column), std::move(null_map)),
            make_nullable(std::make_shared<DataTypeInt32>()), "nullable_int"};
}

ColumnWithTypeAndName create_percentile_array_const_column(const std::vector<double>& quantiles) {
    auto nested = ColumnFloat64::create();
    auto null_map = ColumnUInt8::create();
    auto offsets = ColumnArray::ColumnOffsets::create();
    for (double q : quantiles) {
        nested->insert_value(q);
        null_map->insert_value(0);
    }
    offsets->insert(Field::create_field<TYPE_UINT64>(quantiles.size()));
    auto array = ColumnArray::create(ColumnNullable::create(std::move(nested), std::move(null_map)),
                                     std::move(offsets));
    return {ColumnConst::create(std::move(array), 1),
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat64>())),
            "quantiles"};
}

double read_result(AggregateFunctionPtr fn, AggregateDataPtr place) {
    auto result_column = ColumnFloat64::create();
    fn->insert_result_into(place, *result_column);
    return result_column->get_element(0);
}

} // namespace

TEST(AggregateFunctionPercentileTest, optimized_single_place_paths) {
    auto fn = create_percentile_reservoir_function(
            {std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()});
    ASSERT_TRUE(fn != nullptr);

    std::vector<ColumnWithTypeAndName> arguments;
    arguments.emplace_back(create_value_block({1.0, 2.0, 3.0, 4.0}));
    arguments.emplace_back(create_const_level(0.5));
    ASSERT_EQ(fn->get_const_argument_indexes(), (std::vector<size_t> {1}));

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

TEST(AggregateFunctionPercentileTest, reject_invalid_const_level) {
    auto fn = create_percentile_reservoir_function(
            {std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()});
    ASSERT_TRUE(fn != nullptr);

    std::vector<ColumnWithTypeAndName> arguments(2);
    arguments[0] = create_value_block({1.0});
    arguments[1] = create_const_level(2.0);

    Arena arena;
    std::unique_ptr<char[]> place_mem(new char[fn->size_of_data()]);
    AggregateDataPtr place = place_mem.get();
    fn->create(place);

    const IColumn* columns[] = {arguments[0].column.get(), arguments[1].column.get()};
    try {
        fn->add_batch_single_place(1, place, columns, arena);
        FAIL() << "Expected invalid const level to throw";
    } catch (const Exception& e) {
        ASSERT_NE(e.to_string().find("quantile in func percentile should in [0, 1]"),
                  std::string::npos);
    }

    fn->destroy(place);
}

TEST(AggregateFunctionPercentileTest, nullable_const_null_short_circuit) {
    for (bool use_null_v2 : {false, true}) {
        auto fn = create_percentile_reservoir_function(
                {std::make_shared<DataTypeFloat64>(),
                 make_nullable(std::make_shared<DataTypeFloat64>())},
                false, use_null_v2);
        ASSERT_TRUE(fn != nullptr);

        std::vector<ColumnWithTypeAndName> arguments;
        arguments.emplace_back(create_value_block({1.0, 2.0}));
        arguments.emplace_back(create_const_nullable_null_level(2));
        const IColumn* columns[] = {arguments[0].column.get(), arguments[1].column.get()};

        Arena arena;
        std::unique_ptr<char[]> place_mem(new char[fn->size_of_data()]);
        AggregateDataPtr place = place_mem.get();
        fn->create(place);

        fn->add_batch_single_place(2, place, columns, arena);
        EXPECT_TRUE(std::isnan(read_result(fn, place)));
        fn->destroy(place);
    }
}

TEST(AggregateFunctionPercentileTest, nullable_row_null_short_circuit) {
    for (bool use_null_v2 : {false, true}) {
        auto fn = create_count_function({make_nullable(std::make_shared<DataTypeInt32>())},
                                        use_null_v2);
        ASSERT_TRUE(fn != nullptr);

        std::vector<ColumnWithTypeAndName> arguments;
        arguments.emplace_back(create_nullable_int_column({10, 20, 30}, {1, 0, 0}));
        const IColumn* columns[] = {arguments[0].column.get()};

        Arena arena;
        std::unique_ptr<char[]> place_mem(new char[fn->size_of_data()]);
        AggregateDataPtr place = place_mem.get();
        fn->create(place);

        fn->add_batch_single_place(3, place, columns, arena);
        auto result_column = ColumnInt64::create();
        fn->insert_result_into(place, *result_column);
        EXPECT_EQ(result_column->get_element(0), 2);
        fn->destroy(place);
    }
}

TEST(AggregateFunctionPercentileTest, nullable_unary_add_skips_null_rows) {
    for (bool use_null_v2 : {false, true}) {
        auto fn = create_sum_function({make_nullable(std::make_shared<DataTypeInt32>())},
                                      use_null_v2);
        ASSERT_TRUE(fn != nullptr);

        std::vector<ColumnWithTypeAndName> arguments;
        arguments.emplace_back(create_nullable_int_column({10, 20}, {1, 0}));
        const IColumn* columns[] = {arguments[0].column.get()};

        Arena arena;
        std::unique_ptr<char[]> place_mem(new char[fn->size_of_data()]);
        AggregateDataPtr place = place_mem.get();
        fn->create(place);

        fn->add(place, columns, 0, arena);
        fn->add(place, columns, 1, arena);

        auto result_column = ColumnInt64::create();
        fn->insert_result_into(place, *result_column);
        EXPECT_EQ(result_column->get_element(0), 20);
        fn->destroy(place);
    }
}

TEST(AggregateFunctionPercentileTest, percentile_array_v2_add_batch_range) {
    auto fn = create_percentile_array_v2_function();
    ASSERT_TRUE(fn != nullptr);

    auto source = ColumnFloat64::create();
    for (double value : {1.0, 2.0, 3.0, 4.0}) {
        source->insert_value(value);
    }
    auto quantiles = create_percentile_array_const_column({0.25, 0.5, 0.75});

    const IColumn* columns[] = {source.get(), quantiles.column.get()};

    Arena arena;
    std::unique_ptr<char[]> place_mem(new char[fn->size_of_data()]);
    AggregateDataPtr place = place_mem.get();
    fn->create(place);

    fn->add_batch_range(1, 3, place, columns, arena, false);

    auto result_column = ColumnArray::create(
            ColumnNullable::create(ColumnFloat64::create(), ColumnUInt8::create()),
            ColumnArray::ColumnOffsets::create());
    fn->insert_result_into(place, *result_column);

    const auto* result_array = result_column.get();
    const auto& result_nullable = assert_cast<const ColumnNullable&>(result_array->get_data());
    const auto& result_values =
            assert_cast<const ColumnFloat64&>(result_nullable.get_nested_column()).get_data();
    const auto& result_null_map = result_nullable.get_null_map_data();

    ASSERT_EQ(result_array->get_offsets().size(), 1);
    EXPECT_EQ(result_array->get_offsets()[0], 3);
    ASSERT_EQ(result_values.size(), 3);
    EXPECT_DOUBLE_EQ(result_values[0], 2.5);
    EXPECT_DOUBLE_EQ(result_values[1], 3.0);
    EXPECT_DOUBLE_EQ(result_values[2], 3.5);
    ASSERT_EQ(result_null_map.size(), 3);
    for (auto is_null : result_null_map) {
        EXPECT_EQ(is_null, 0);
    }

    fn->destroy(place);
}

} // namespace doris
