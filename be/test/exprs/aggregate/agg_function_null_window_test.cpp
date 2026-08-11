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

// Regression tests for the optimization that narrowed the has_null() scan in
// AggregateFunctionNullUnaryInline::add_range_single_place and
// ::execute_function_with_incremental from the whole buffered column (O(n))
// down to just the frame range that is actually touched (O(frame)).
// These tests pin down that the narrower range check still finds every null
// that matters, including nulls that sit outside the checked sub-range but
// inside the full column, and nulls that sit exactly on a frame boundary.

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/primitive_type.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"

namespace doris {

void register_aggregate_function_sum(AggregateFunctionSimpleFactory& factory);

namespace {

// Builds a nullable Int32 column, marking `null_positions` as NULL.
ColumnPtr build_nullable_int32_column(const std::vector<int32_t>& values,
                                      const std::vector<size_t>& null_positions) {
    auto data_column = ColumnInt32::create();
    auto null_map = ColumnUInt8::create();
    std::vector<uint8_t> is_null(values.size(), 0);
    for (auto pos : null_positions) {
        is_null[pos] = 1;
    }
    for (size_t i = 0; i < values.size(); ++i) {
        data_column->insert_value(values[i]);
        null_map->insert_value(is_null[i]);
    }
    return ColumnNullable::create(std::move(data_column), std::move(null_map));
}

AggregateFunctionPtr create_window_sum() {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_sum(factory);
    DataTypes data_types = {make_nullable(std::make_shared<DataTypeInt32>())};
    return factory.get("sum", data_types, nullptr, true, -1,
                       {.is_window_function = true, .column_names = {}});
}

int64_t naive_sum_skip_null(const std::vector<int32_t>& values,
                            const std::vector<size_t>& null_positions, int64_t begin, int64_t end) {
    std::vector<uint8_t> is_null(values.size(), 0);
    for (auto pos : null_positions) {
        is_null[pos] = 1;
    }
    int64_t sum = 0;
    for (int64_t i = begin; i < end; ++i) {
        if (!is_null[i]) {
            sum += values[i];
        }
    }
    return sum;
}

int64_t read_sum_result(const AggregateFunctionPtr& agg_function, AggregateDataPtr place) {
    auto nested_data_type = std::make_shared<DataTypeInt64>();
    auto result_nested = ColumnInt64::create();
    auto result_null_map = ColumnUInt8::create();
    auto result_column =
            ColumnNullable::create(std::move(result_nested), std::move(result_null_map));
    agg_function->insert_result_into(place, *result_column);
    EXPECT_EQ(result_column->get_null_map_data().back(), 0);
    return assert_cast<const ColumnInt64&>(result_column->get_nested_column()).get_element(0);
}

} // namespace

// The whole column has a null, but it sits *outside* the queried frame.
// Before the fix, has_null() scanned the entire buffered column and always
// took the slow per-row `add()` path whenever any null existed anywhere.
// After the fix, has_null(begin, end) is scoped to the frame, so a frame
// with no null in range should take the fast nested add_range_single_place
// path and still produce the same (correct) sum.
TEST(AggFunctionNullWindowTest, AddRangeSinglePlaceIgnoresNullOutsideFrame) {
    std::vector<int32_t> values = {10, 20, 30, 40, 50};
    std::vector<size_t> null_positions = {3}; // value 40 is NULL
    auto column = build_nullable_int32_column(values, null_positions);
    const IColumn* columns[1] = {column.get()};

    auto agg_function = create_window_sum();
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);
    Arena arena;

    UInt8 use_null_result = 0;
    UInt8 could_use_previous_result = 0;
    // frame [0, 3) does not touch the null at index 3.
    agg_function->add_range_single_place(0, 5, 0, 3, place, columns, arena, &use_null_result,
                                         &could_use_previous_result);

    EXPECT_FALSE(use_null_result);
    EXPECT_EQ(read_sum_result(agg_function, place),
              naive_sum_skip_null(values, null_positions, 0, 3));
    agg_function->destroy(place);
}

// The frame itself contains a null in the middle; the narrowed has_null()
// check must still detect it and fall back to the per-row add() path that
// correctly skips the null.
TEST(AggFunctionNullWindowTest, AddRangeSinglePlaceSkipsNullInsideFrame) {
    std::vector<int32_t> values = {10, 20, 30, 40, 50};
    std::vector<size_t> null_positions = {3}; // value 40 is NULL
    auto column = build_nullable_int32_column(values, null_positions);
    const IColumn* columns[1] = {column.get()};

    auto agg_function = create_window_sum();
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);
    Arena arena;

    UInt8 use_null_result = 0;
    UInt8 could_use_previous_result = 0;
    // frame [2, 5) covers index 3, which is NULL.
    agg_function->add_range_single_place(0, 5, 2, 5, place, columns, arena, &use_null_result,
                                         &could_use_previous_result);

    EXPECT_FALSE(use_null_result);
    EXPECT_EQ(read_sum_result(agg_function, place),
              naive_sum_skip_null(values, null_positions, 2, 5));
    agg_function->destroy(place);
}

// A null sitting exactly at the first position of the frame must still be
// detected (guards against an off-by-one in the narrowed begin index).
TEST(AggFunctionNullWindowTest, AddRangeSinglePlaceDetectsNullAtFrameStart) {
    std::vector<int32_t> values = {1, 2, 3, 4, 5};
    std::vector<size_t> null_positions = {0};
    auto column = build_nullable_int32_column(values, null_positions);
    const IColumn* columns[1] = {column.get()};

    auto agg_function = create_window_sum();
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);
    Arena arena;

    UInt8 use_null_result = 0;
    UInt8 could_use_previous_result = 0;
    // frame [0, 2) starts exactly at the NULL row.
    agg_function->add_range_single_place(0, 5, 0, 2, place, columns, arena, &use_null_result,
                                         &could_use_previous_result);

    EXPECT_FALSE(use_null_result);
    EXPECT_EQ(read_sum_result(agg_function, place),
              naive_sum_skip_null(values, null_positions, 0, 2));
    agg_function->destroy(place);
}

// End-to-end simulation of the analytic sink's sliding-window driver
// (see AnalyticSinkLocalState::_execute_for_function), which always passes
// previous_is_nul=false, end_is_nul=false, has_null=false into
// execute_function_with_incremental and lets the Nullable wrapper compute
// null-awareness itself from the null map. This exercises the widened
// has_null(frame_start - 1, current_frame_end) check added by the fix,
// which must cover both the outgoing (frame_start - 1) and incoming
// (frame_end - 1) positions as the frame slides across scattered nulls,
// including nulls landing exactly on those boundary positions.
TEST(AggFunctionNullWindowTest, ExecuteFunctionWithIncrementalSlidingWindowMatchesNaive) {
    std::vector<int32_t> values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
    // Nulls scattered so that, as a size-3 window slides, they land at the
    // outgoing position, the incoming position, and mid-frame at various steps.
    std::vector<size_t> null_positions = {2, 5, 6, 9};
    auto column = build_nullable_int32_column(values, null_positions);
    const IColumn* columns[1] = {column.get()};

    auto agg_function = create_window_sum();
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);
    Arena arena;

    const int64_t partition_start = 0;
    const auto partition_end = static_cast<int64_t>(values.size());
    const int64_t window_size = 3;

    UInt8 use_null_result = 0;
    UInt8 could_use_previous_result = 0;

    for (int64_t frame_start = 0; frame_start + window_size <= partition_end; ++frame_start) {
        int64_t frame_end = frame_start + window_size;
        agg_function->execute_function_with_incremental(
                partition_start, partition_end, frame_start, frame_end, place, columns, arena,
                /*previous_is_nul*/ false, /*end_is_nul*/ false, /*has_null*/ false,
                &use_null_result, &could_use_previous_result);

        int64_t expected = naive_sum_skip_null(values, null_positions, frame_start, frame_end);
        bool expect_null = false;
        {
            std::vector<uint8_t> is_null(values.size(), 0);
            for (auto pos : null_positions) {
                is_null[pos] = 1;
            }
            expect_null = true;
            for (int64_t i = frame_start; i < frame_end; ++i) {
                if (!is_null[i]) {
                    expect_null = false;
                    break;
                }
            }
        }
        ASSERT_EQ(use_null_result, expect_null)
                << "frame [" << frame_start << ", " << frame_end << ")";
        if (!use_null_result) {
            EXPECT_EQ(read_sum_result(agg_function, place), expected)
                    << "frame [" << frame_start << ", " << frame_end << ")";
        }
    }
    agg_function->destroy(place);
}

} // namespace doris
