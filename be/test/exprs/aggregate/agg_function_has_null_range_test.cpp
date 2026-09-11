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

#include <algorithm>
#include <memory>
#include <vector>

#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exprs/aggregate/aggregate_function_null_v2.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"

namespace doris {

void register_aggregate_function_count(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_avg(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_sum(AggregateFunctionSimpleFactory& factory);

namespace {

ColumnPtr create_nullable_int32_column(const std::vector<int32_t>& values,
                                       const std::vector<size_t>& null_positions) {
    auto data_column = ColumnInt32::create();
    auto null_map = ColumnUInt8::create(values.size(), 0);
    for (auto value : values) {
        data_column->insert_value(value);
    }
    for (auto position : null_positions) {
        null_map->get_data()[position] = 1;
    }
    return ColumnNullable::create(std::move(data_column), std::move(null_map));
}

struct TrackingSumData {
    int64_t sum = 0;
};

class TrackingSum final : public IAggregateFunctionDataHelper<TrackingSumData, TrackingSum>,
                          public UnaryExpression,
                          public NullableAggregateFunction {
public:
    explicit TrackingSum(const DataTypes& argument_types)
            : IAggregateFunctionDataHelper<TrackingSumData, TrackingSum>(argument_types) {}

    String get_name() const override { return "tracking_sum"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        ++add_calls;
        data(place).sum += assert_cast<const ColumnInt32&, TypeCheckOnRelease::DISABLE>(*columns[0])
                                   .get_element(row_num);
    }

    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena& arena) const override {
        ++batch_calls;
        for (size_t row_num = 0; row_num < batch_size; ++row_num) {
            data(place).sum +=
                    assert_cast<const ColumnInt32&, TypeCheckOnRelease::DISABLE>(*columns[0])
                            .get_element(row_num);
        }
    }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena& arena, UInt8* use_null_result,
                                UInt8* could_use_previous_result) const override {
        ++range_calls;
        frame_start = std::max(frame_start, partition_start);
        frame_end = std::min(frame_end, partition_end);
        for (int64_t row_num = frame_start; row_num < frame_end; ++row_num) {
            data(place).sum +=
                    assert_cast<const ColumnInt32&, TypeCheckOnRelease::DISABLE>(*columns[0])
                            .get_element(row_num);
        }
        *use_null_result = frame_start >= frame_end;
        *could_use_previous_result = frame_start < frame_end;
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena&) const override {
        data(place).sum += data(rhs).sum;
    }

    void serialize(ConstAggregateDataPtr place, BufferWritable& buffer) const override {
        buffer.write_binary(data(place).sum);
    }

    void deserialize(AggregateDataPtr place, BufferReadable& buffer, Arena&) const override {
        buffer.read_binary(data(place).sum);
    }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        assert_cast<ColumnInt64&, TypeCheckOnRelease::DISABLE>(to).insert_value(data(place).sum);
    }

    static void reset_counters() {
        add_calls = 0;
        batch_calls = 0;
        range_calls = 0;
    }

    static inline size_t add_calls = 0;
    static inline size_t batch_calls = 0;
    static inline size_t range_calls = 0;
};

using TrackingNullableV2 = AggregateFunctionNullUnaryInlineV2<TrackingSum, true>;

std::unique_ptr<TrackingNullableV2> create_tracking_nullable_v2(bool is_window_function) {
    DataTypes nested_types = {std::make_shared<DataTypeInt32>()};
    DataTypes nullable_types = {make_nullable(std::make_shared<DataTypeInt32>())};
    return std::make_unique<TrackingNullableV2>(new TrackingSum(nested_types), nullable_types,
                                                is_window_function);
}

int64_t read_nullable_int64_result(const IAggregateFunction& function,
                                   ConstAggregateDataPtr place) {
    auto result = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    function.insert_result_into(place, *result);
    EXPECT_FALSE(result->is_null_at(0));
    return assert_cast<const ColumnInt64&>(result->get_nested_column()).get_element(0);
}

AggregateFunctionPtr create_window_sum_v2() {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_sum(factory);
    DataTypes data_types = {make_nullable(std::make_shared<DataTypeInt32>())};
    return factory.get("sum", data_types, nullptr, true, -1,
                       {.is_window_function = true,
                        .enable_aggregate_function_null_v2 = true,
                        .column_names = {}});
}

AggregateFunctionPtr create_window_avg_v2() {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_avg(factory);
    DataTypes data_types = {make_nullable(std::make_shared<DataTypeInt32>())};
    return factory.get("avg", data_types, nullptr, true, -1,
                       {.is_window_function = true,
                        .enable_aggregate_function_null_v2 = true,
                        .column_names = {}});
}

int64_t evaluate_window_count(const ColumnPtr& column, int64_t partition_start,
                              int64_t partition_end, int64_t frame_start, int64_t frame_end) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_count(factory);
    DataTypes data_types = {make_nullable(std::make_shared<DataTypeInt32>())};
    auto function = factory.get("count", data_types, nullptr, false, -1,
                                {.is_window_function = true, .column_names = {}});
    EXPECT_NE(function, nullptr);

    Arena arena;
    auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(function->size_of_data()));
    function->create(place);
    const IColumn* columns[] = {column.get()};
    UInt8 use_null_result = false;
    UInt8 could_use_previous_result = false;
    function->add_range_single_place(partition_start, partition_end, frame_start, frame_end, place,
                                     columns, arena, &use_null_result, &could_use_previous_result);
    auto result = ColumnInt64::create();
    function->insert_result_into(place, *result);
    function->destroy(place);
    return result->get_element(0);
}

} // namespace

TEST(AggregateFunctionHasNullRangeTest, NullableV2BatchIgnoresNullOutsideBatch) {
    auto column = create_nullable_int32_column({1, 2, 3, 4, 5}, {4});
    const IColumn* columns[] = {column.get()};
    auto function = create_tracking_nullable_v2(false);
    Arena arena;
    auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(function->size_of_data()));
    function->create(place);

    TrackingSum::reset_counters();
    function->add_batch_single_place(3, place, columns, arena);

    EXPECT_EQ(TrackingSum::batch_calls, 1);
    EXPECT_EQ(TrackingSum::add_calls, 0);
    EXPECT_EQ(read_nullable_int64_result(*function, place), 6);
    function->destroy(place);
}

TEST(AggregateFunctionHasNullRangeTest, NullableV2WindowIgnoresNullOutsideFrame) {
    auto column = create_nullable_int32_column({1, 2, 3, 4, 5}, {4});
    const IColumn* columns[] = {column.get()};
    auto function = create_tracking_nullable_v2(true);
    Arena arena;
    auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(function->size_of_data()));
    function->create(place);
    UInt8 use_null_result = false;
    UInt8 could_use_previous_result = false;

    TrackingSum::reset_counters();
    function->add_range_single_place(0, 5, 0, 3, place, columns, arena, &use_null_result,
                                     &could_use_previous_result);

    EXPECT_EQ(TrackingSum::range_calls, 1);
    EXPECT_EQ(TrackingSum::add_calls, 0);
    EXPECT_EQ(read_nullable_int64_result(*function, place), 6);
    function->destroy(place);
}

TEST(AggregateFunctionHasNullRangeTest, NullableV2WindowDetectsNullInsideFrame) {
    auto column = create_nullable_int32_column({1, 2, 3, 4, 5}, {1, 4});
    const IColumn* columns[] = {column.get()};
    auto function = create_tracking_nullable_v2(true);
    Arena arena;
    auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(function->size_of_data()));
    function->create(place);
    UInt8 use_null_result = false;
    UInt8 could_use_previous_result = false;

    TrackingSum::reset_counters();
    function->add_range_single_place(0, 5, 0, 3, place, columns, arena, &use_null_result,
                                     &could_use_previous_result);

    EXPECT_EQ(TrackingSum::range_calls, 0);
    EXPECT_EQ(TrackingSum::add_calls, 2);
    EXPECT_EQ(read_nullable_int64_result(*function, place), 4);
    function->destroy(place);
}

TEST(AggregateFunctionHasNullRangeTest, NullableV2IncrementalWindowMatchesExpectedResults) {
    auto column = create_nullable_int32_column({1, 2, 3, 4, 5, 6, 7, 8}, {2, 5});
    const IColumn* columns[] = {column.get()};
    auto function = create_window_sum_v2();
    ASSERT_NE(function, nullptr);
    Arena arena;
    auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(function->size_of_data()));
    function->create(place);
    UInt8 use_null_result = false;
    UInt8 could_use_previous_result = false;
    const std::vector<int64_t> expected = {3, 6, 9, 9, 12, 15};

    for (int64_t frame_start = 0; frame_start < 6; ++frame_start) {
        function->execute_function_with_incremental(0, 8, frame_start, frame_start + 3, place,
                                                    columns, arena, false, false, false,
                                                    &use_null_result, &could_use_previous_result);
        ASSERT_FALSE(use_null_result);
        EXPECT_EQ(read_nullable_int64_result(*function, place), expected[frame_start]);
    }
    function->destroy(place);
}

TEST(AggregateFunctionHasNullRangeTest, NullableV2EmptyTrailingSlicePreservesNullResult) {
    auto column = create_nullable_int32_column({1}, {0});
    const IColumn* columns[] = {column.get()};
    auto function = create_window_avg_v2();
    ASSERT_NE(function, nullptr);
    Arena arena;
    auto* place = reinterpret_cast<AggregateDataPtr>(arena.alloc(function->size_of_data()));
    function->create(place);
    UInt8 use_null_result = false;
    UInt8 could_use_previous_result = false;

    function->add_range_single_place(0, 1, 0, 2, place, columns, arena, &use_null_result,
                                     &could_use_previous_result);
    ASSERT_TRUE(could_use_previous_result);
    ASSERT_FALSE(use_null_result);

    function->add_range_single_place(0, 1, 2, 3, place, columns, arena, &use_null_result,
                                     &could_use_previous_result);

    auto result = ColumnNullable::create(ColumnFloat64::create(), ColumnUInt8::create());
    function->insert_result_into(place, *result);
    EXPECT_TRUE(result->is_null_at(0));
    function->destroy(place);
}

TEST(AggregateFunctionHasNullRangeTest, CountNullableUsesOnlyCurrentFrame) {
    auto column = create_nullable_int32_column({1, 2, 3, 4, 5, 6}, {1, 5});

    EXPECT_EQ(evaluate_window_count(column, 0, 6, 2, 5), 3);
    EXPECT_EQ(evaluate_window_count(column, 0, 6, 0, 3), 2);
    EXPECT_EQ(evaluate_window_count(column, 1, 5, -2, 8), 3);
    EXPECT_EQ(evaluate_window_count(column, 0, 6, 6, 6), 0);
}

} // namespace doris
