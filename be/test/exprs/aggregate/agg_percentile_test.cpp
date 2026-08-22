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
#include <limits>
#include <memory>
#include <optional>
#include <vector>

#include "common/exception.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/field.h"
#include "core/string_buffer.hpp"
#include "exprs/aggregate/aggregate_function_percentile.h"
#include "exprs/aggregate/aggregate_function_percentile_reservoir.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "util/tdigest.h"

namespace doris {

namespace {

AggregateFunctionPtr create_percentile_approx_array_function(bool has_compression) {
    DataTypes argument_types {
            std::make_shared<DataTypeFloat64>(),
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat64>()))};
    if (has_compression) {
        argument_types.push_back(std::make_shared<DataTypeFloat64>());
    }
    auto result_type =
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat64>()));
    return AggregateFunctionSimpleFactory::instance().get(
            "percentile_approx_array", argument_types, result_type, false,
            BeExecVersionManager::get_newest_version(), {.column_names = {}});
}

MutableColumnPtr create_values(const std::vector<double>& values) {
    auto column = ColumnFloat64::create();
    for (double value : values) {
        column->insert_value(value);
    }
    return column;
}

MutableColumnPtr create_quantiles(const std::vector<double>& quantiles, size_t rows,
                                  std::optional<size_t> null_index = std::nullopt) {
    auto nested_column = ColumnFloat64::create();
    auto null_map = ColumnUInt8::create();
    auto offsets = ColumnArray::ColumnOffsets::create();
    for (size_t row = 0; row < rows; ++row) {
        for (size_t i = 0; i < quantiles.size(); ++i) {
            nested_column->insert_value(quantiles[i]);
            null_map->insert_value(null_index.has_value() && *null_index == i);
        }
        offsets->insert_value((row + 1) * quantiles.size());
    }
    return ColumnArray::create(
            ColumnNullable::create(std::move(nested_column), std::move(null_map)),
            std::move(offsets));
}

std::vector<double> read_result(const AggregateFunctionPtr& function, AggregateDataPtr place) {
    auto result_column = ColumnArray::create(
            ColumnNullable::create(ColumnFloat64::create(), ColumnUInt8::create()),
            ColumnArray::ColumnOffsets::create());
    function->insert_result_into(place, *result_column);
    const auto& nullable_data = assert_cast<const ColumnNullable&>(result_column->get_data());
    const auto& data = assert_cast<const ColumnFloat64&>(nullable_data.get_nested_column());
    return {data.get_data().begin(), data.get_data().end()};
}

std::vector<double> expected_quantiles(const std::vector<double>& values,
                                       const std::vector<double>& quantiles, float compression) {
    TDigest digest(compression);
    for (double value : values) {
        digest.add(value);
    }
    std::vector<double> result;
    result.reserve(quantiles.size());
    for (double quantile : quantiles) {
        result.push_back(digest.quantile(quantile));
    }
    return result;
}

void expect_results_equal(const std::vector<double>& actual, const std::vector<double>& expected) {
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < actual.size(); ++i) {
        EXPECT_DOUBLE_EQ(actual[i], expected[i]);
    }
}
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

ColumnWithTypeAndName create_const_nullable_level(double value, size_t size) {
    auto level_column = ColumnFloat64::create();
    level_column->insert_value(value);
    auto null_map = ColumnUInt8::create();
    null_map->insert_value(0);
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

double read_scalar_result(AggregateFunctionPtr fn, AggregateDataPtr place) {
    auto result_column = ColumnFloat64::create();
    fn->insert_result_into(place, *result_column);
    return result_column->get_element(0);
}

} // namespace

TEST(AggregateFunctionPercentileTest, AddAndBatchPaths) {
    const std::vector<double> values {1, 2, 3, 4, 5, 100};
    const std::vector<double> quantiles {0.9, 0.0, 0.5, 0.5, 1.0};
    auto function = create_percentile_approx_array_function(false);
    ASSERT_NE(function, nullptr);

    auto value_column = create_values(values);
    auto quantile_column = create_quantiles(quantiles, values.size());
    const IColumn* columns[] = {value_column.get(), quantile_column.get()};
    Arena arena;

    std::unique_ptr<char[]> row_memory(new char[function->size_of_data()]);
    AggregateDataPtr row_place = row_memory.get();
    function->create(row_place);
    for (size_t i = 0; i < values.size(); ++i) {
        function->add(row_place, columns, i, arena);
    }

    std::unique_ptr<char[]> batch_memory(new char[function->size_of_data()]);
    AggregateDataPtr batch_place = batch_memory.get();
    function->create(batch_place);
    function->add_batch_single_place(values.size(), batch_place, columns, arena);

    std::unique_ptr<char[]> range_memory(new char[function->size_of_data()]);
    AggregateDataPtr range_place = range_memory.get();
    function->create(range_place);
    function->add_batch_range(1, 4, range_place, columns, arena, false);

    const auto expected = expected_quantiles(values, quantiles, 10000);
    expect_results_equal(read_result(function, row_place), expected);
    expect_results_equal(read_result(function, batch_place), expected);
    expect_results_equal(read_result(function, range_place),
                         expected_quantiles({2, 3, 4, 5}, quantiles, 10000));

    function->destroy(row_place);
    function->destroy(batch_place);
    function->destroy(range_place);
}

TEST(AggregateFunctionPercentileTest, CompressionSerializationAndMerge) {
    const std::vector<double> values {5, 10, 15, 20, 25, 30};
    const std::vector<double> quantiles {0.25, 0.5, 0.75};
    auto function = create_percentile_approx_array_function(true);
    ASSERT_NE(function, nullptr);

    auto value_column = create_values(values);
    auto quantile_column = create_quantiles(quantiles, values.size());
    auto compression_column = ColumnFloat64::create();
    compression_column->insert_value(2048);
    const IColumn* columns[] = {value_column.get(), quantile_column.get(),
                                compression_column.get()};
    Arena arena;

    std::unique_ptr<char[]> source_memory(new char[function->size_of_data()]);
    AggregateDataPtr source_place = source_memory.get();
    function->create(source_place);
    function->add_batch_single_place(values.size(), source_place, columns, arena);

    ColumnString serialized_column;
    VectorBufferWriter writer(serialized_column);
    function->serialize(source_place, writer);
    writer.commit();

    std::unique_ptr<char[]> restored_memory(new char[function->size_of_data()]);
    AggregateDataPtr restored_place = restored_memory.get();
    function->create(restored_place);
    VectorBufferReader reader(serialized_column.get_data_at(0));
    function->deserialize(restored_place, reader, arena);

    std::unique_ptr<char[]> merged_memory(new char[function->size_of_data()]);
    AggregateDataPtr merged_place = merged_memory.get();
    function->create(merged_place);
    function->merge(merged_place, restored_place, arena);

    const auto expected = expected_quantiles(values, quantiles, 2048);
    expect_results_equal(read_result(function, restored_place), expected);
    expect_results_equal(read_result(function, merged_place), expected);

    function->destroy(source_place);
    function->destroy(restored_place);
    function->destroy(merged_place);
}

TEST(AggregateFunctionPercentileTest, MergeCompatibleInitializedStates) {
    const std::vector<double> left_values {5, 10, 15};
    const std::vector<double> right_values {20, 25, 30};
    const std::vector<double> quantiles {0.25, 0.5, 0.75};
    auto function = create_percentile_approx_array_function(true);
    ASSERT_NE(function, nullptr);
    Arena arena;

    auto create_state = [&](const std::vector<double>& values) {
        auto value_column = create_values(values);
        auto quantile_column = create_quantiles(quantiles, values.size());
        auto compression_column = ColumnFloat64::create();
        compression_column->insert_value(2048);
        const IColumn* columns[] = {value_column.get(), quantile_column.get(),
                                    compression_column.get()};
        std::unique_ptr<char[]> memory(new char[function->size_of_data()]);
        function->create(memory.get());
        function->add_batch_single_place(values.size(), memory.get(), columns, arena);
        return memory;
    };

    auto left = create_state(left_values);
    auto right = create_state(right_values);
    function->merge(left.get(), right.get(), arena);

    std::vector<double> all_values = left_values;
    all_values.insert(all_values.end(), right_values.begin(), right_values.end());
    expect_results_equal(read_result(function, left.get()),
                         expected_quantiles(all_values, quantiles, 2048));

    function->destroy(left.get());
    function->destroy(right.get());
}

TEST(AggregateFunctionPercentileTest, EmptyQuantilesAndInvalidQuantile) {
    auto function = create_percentile_approx_array_function(false);
    ASSERT_NE(function, nullptr);
    Arena arena;

    auto value_column = create_values({1, 2, 3});
    auto empty_quantile_column = create_quantiles({}, 3);
    const IColumn* empty_columns[] = {value_column.get(), empty_quantile_column.get()};
    std::unique_ptr<char[]> empty_memory(new char[function->size_of_data()]);
    AggregateDataPtr empty_place = empty_memory.get();
    function->create(empty_place);
    function->add_batch_single_place(3, empty_place, empty_columns, arena);
    EXPECT_TRUE(read_result(function, empty_place).empty());
    const auto& empty_state = *reinterpret_cast<const PercentileApproxArrayState*>(empty_place);
    EXPECT_TRUE(empty_state.init_flag);
    EXPECT_EQ(empty_state.digest.get(), nullptr);

    ColumnString serialized_column;
    VectorBufferWriter writer(serialized_column);
    function->serialize(empty_place, writer);
    writer.commit();
    std::unique_ptr<char[]> restored_memory(new char[function->size_of_data()]);
    AggregateDataPtr restored_place = restored_memory.get();
    function->create(restored_place);
    VectorBufferReader reader(serialized_column.get_data_at(0));
    function->deserialize(restored_place, reader, arena);
    const auto& restored_state =
            *reinterpret_cast<const PercentileApproxArrayState*>(restored_place);
    EXPECT_TRUE(restored_state.init_flag);
    EXPECT_EQ(restored_state.digest.get(), nullptr);
    EXPECT_TRUE(read_result(function, restored_place).empty());

    std::unique_ptr<char[]> merged_memory(new char[function->size_of_data()]);
    AggregateDataPtr merged_place = merged_memory.get();
    function->create(merged_place);
    function->merge(merged_place, restored_place, arena);
    const auto& merged_state = *reinterpret_cast<const PercentileApproxArrayState*>(merged_place);
    EXPECT_TRUE(merged_state.init_flag);
    EXPECT_EQ(merged_state.digest.get(), nullptr);
    EXPECT_TRUE(read_result(function, merged_place).empty());

    auto invalid_quantile_column = create_quantiles({0.5, 0.9}, 3, 1);
    const IColumn* invalid_columns[] = {value_column.get(), invalid_quantile_column.get()};
    std::unique_ptr<char[]> invalid_memory(new char[function->size_of_data()]);
    AggregateDataPtr invalid_place = invalid_memory.get();
    function->create(invalid_place);
    EXPECT_THROW(function->add_batch_single_place(3, invalid_place, invalid_columns, arena),
                 Exception);

    function->destroy(empty_place);
    function->destroy(restored_place);
    function->destroy(merged_place);
    function->destroy(invalid_place);
}

TEST(AggregateFunctionPercentileTest, RejectsIncompatibleStates) {
    auto function = create_percentile_approx_array_function(true);
    ASSERT_NE(function, nullptr);
    Arena arena;

    auto create_state = [&](const std::vector<double>& quantiles, double compression) {
        auto value_column = create_values({1, 2, 3});
        auto quantile_column = create_quantiles(quantiles, 3);
        auto compression_column = ColumnFloat64::create();
        compression_column->insert_value(compression);
        const IColumn* columns[] = {value_column.get(), quantile_column.get(),
                                    compression_column.get()};
        std::unique_ptr<char[]> memory(new char[function->size_of_data()]);
        function->create(memory.get());
        function->add_batch_single_place(3, memory.get(), columns, arena);
        return memory;
    };

    auto destination = create_state({0.5}, 2048);
    auto different_quantiles = create_state({0.9}, 2048);
    auto different_compression = create_state({0.5}, 10000);
    EXPECT_THROW(function->merge(destination.get(), different_quantiles.get(), arena), Exception);
    EXPECT_THROW(function->merge(destination.get(), different_compression.get(), arena), Exception);

    function->destroy(destination.get());
    function->destroy(different_quantiles.get());
    function->destroy(different_compression.get());
}

TEST(AggregateFunctionPercentileTest, HandlesNonFiniteParameters) {
    const std::vector<double> non_finite_values {std::numeric_limits<double>::quiet_NaN(),
                                                 std::numeric_limits<double>::infinity(),
                                                 -std::numeric_limits<double>::infinity()};
    auto value_column = create_values({1, 2, 3});
    Arena arena;

    auto function_without_compression = create_percentile_approx_array_function(false);
    ASSERT_NE(function_without_compression, nullptr);
    for (double quantile : non_finite_values) {
        auto quantile_column = create_quantiles({quantile}, 3);
        const IColumn* columns[] = {value_column.get(), quantile_column.get()};
        std::unique_ptr<char[]> memory(new char[function_without_compression->size_of_data()]);
        function_without_compression->create(memory.get());
        EXPECT_THROW(function_without_compression->add_batch_single_place(3, memory.get(), columns,
                                                                          arena),
                     Exception);
        function_without_compression->destroy(memory.get());
    }

    auto function_with_compression = create_percentile_approx_array_function(true);
    ASSERT_NE(function_with_compression, nullptr);
    auto quantile_column = create_quantiles({0.5}, 3);
    const auto expected = expected_quantiles({1, 2, 3}, {0.5}, 10000);
    for (double compression : non_finite_values) {
        auto compression_column = ColumnFloat64::create();
        compression_column->insert_value(compression);
        const IColumn* columns[] = {value_column.get(), quantile_column.get(),
                                    compression_column.get()};
        std::unique_ptr<char[]> memory(new char[function_with_compression->size_of_data()]);
        function_with_compression->create(memory.get());
        function_with_compression->add_batch_single_place(3, memory.get(), columns, arena);
        const auto& state = *reinterpret_cast<const PercentileApproxArrayState*>(memory.get());
        EXPECT_FLOAT_EQ(state.compressions, 10000);
        expect_results_equal(read_result(function_with_compression, memory.get()), expected);
        function_with_compression->destroy(memory.get());
    }
}

TEST(AggregateFunctionPercentileTest, reservoir_state_caches_const_level_across_reset) {
    QuantileReservoirSampler state;
    state.init(0.5);
    state.add(1.0);
    state.add(3.0);
    EXPECT_DOUBLE_EQ(state.get(), 2.0);

    // Repeated rows and analytic frame resets belong to the same aggregate expression, whose
    // level is a semantic constant. Neither path should replace or revalidate the cached level.
    state.init(1.0);
    state.add(5.0);
    EXPECT_DOUBLE_EQ(state.get(), 3.0);

    state.reset();
    EXPECT_TRUE(state.is_level_initialized());
    state.init(0.0);
    state.add(10.0);
    state.add(20.0);
    EXPECT_DOUBLE_EQ(state.get(), 15.0);
}

TEST(AggregateFunctionPercentileTest, reservoir_state_serialization_preserves_initialization) {
    QuantileReservoirSampler empty_state;
    auto empty_buffer = ColumnString::create();
    BufferWritable empty_writer(*empty_buffer);
    empty_state.serialize(empty_writer);
    empty_writer.commit();

    QuantileReservoirSampler deserialized_empty_state;
    BufferReadable empty_reader(empty_buffer->get_data_at(0));
    deserialized_empty_state.deserialize(empty_reader);
    EXPECT_FALSE(deserialized_empty_state.is_level_initialized());

    QuantileReservoirSampler initialized_state;
    initialized_state.init(0.5);
    initialized_state.add(1.0);
    initialized_state.add(3.0);
    initialized_state.merge(deserialized_empty_state);
    EXPECT_TRUE(initialized_state.is_level_initialized());
    EXPECT_DOUBLE_EQ(initialized_state.get(), 2.0);

    auto initialized_buffer = ColumnString::create();
    BufferWritable initialized_writer(*initialized_buffer);
    initialized_state.serialize(initialized_writer);
    initialized_writer.commit();

    QuantileReservoirSampler deserialized_initialized_state;
    BufferReadable initialized_reader(initialized_buffer->get_data_at(0));
    deserialized_initialized_state.deserialize(initialized_reader);
    EXPECT_TRUE(deserialized_initialized_state.is_level_initialized());
    EXPECT_DOUBLE_EQ(deserialized_initialized_state.get(), 2.0);
}

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
    EXPECT_DOUBLE_EQ(read_scalar_result(fn, place), 2.5);

    fn->reset(place);

    UInt8 use_null_result = false;
    UInt8 could_use_previous_result = false;
    fn->add_range_single_place(0, 4, 1, 3, place, columns, arena, &use_null_result,
                               &could_use_previous_result);
    EXPECT_FALSE(use_null_result);
    EXPECT_TRUE(could_use_previous_result);
    EXPECT_DOUBLE_EQ(read_scalar_result(fn, place), 2.5);

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
        EXPECT_TRUE(std::isnan(read_scalar_result(fn, place)));
        fn->destroy(place);
    }
}

TEST(AggregateFunctionPercentileTest, nullable_const_level_across_rows) {
    for (bool use_null_v2 : {false, true}) {
        auto fn = create_percentile_reservoir_function(
                {std::make_shared<DataTypeFloat64>(),
                 make_nullable(std::make_shared<DataTypeFloat64>())},
                false, use_null_v2);
        ASSERT_TRUE(fn != nullptr);

        std::vector<ColumnWithTypeAndName> arguments;
        arguments.emplace_back(create_value_block({1.0, 2.0, 3.0, 4.0}));
        arguments.emplace_back(create_const_nullable_level(0.5, 4));
        const IColumn* columns[] = {arguments[0].column.get(), arguments[1].column.get()};

        Arena arena;
        std::unique_ptr<char[]> place_mem(new char[fn->size_of_data()]);
        AggregateDataPtr place = place_mem.get();
        fn->create(place);

        fn->add_batch_single_place(4, place, columns, arena);
        EXPECT_DOUBLE_EQ(read_scalar_result(fn, place), 2.5);
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
