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

#include <limits>
#include <memory>
#include <optional>
#include <vector>

#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/string_buffer.hpp"
#include "exprs/aggregate/aggregate_function_percentile.h"
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

} // namespace

TEST(AggregateFunctionPercentileApproxArrayTest, AddAndBatchPaths) {
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

    const auto expected = expected_quantiles(values, quantiles, 10000);
    expect_results_equal(read_result(function, row_place), expected);
    expect_results_equal(read_result(function, batch_place), expected);

    function->destroy(row_place);
    function->destroy(batch_place);
}

TEST(AggregateFunctionPercentileApproxArrayTest, CompressionSerializationAndMerge) {
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

TEST(AggregateFunctionPercentileApproxArrayTest, EmptyQuantilesAndInvalidQuantile) {
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

TEST(AggregateFunctionPercentileApproxArrayTest, RejectsIncompatibleStates) {
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

TEST(AggregateFunctionPercentileApproxArrayTest, HandlesNonFiniteParameters) {
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

} // namespace doris
