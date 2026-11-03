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

#include "core/arena.h"
#include "core/column/column_complex.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_quantilestate.h"
#include "core/value/quantile_state.h"
#include "exprs/aggregate/aggregate_function_quantile_state.h"
#include "util/defer_op.h"
#include "util/tdigest.h"

namespace doris {
namespace {

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

void expect_reset_quantile_state(size_t count, bool is_window) {
    auto type = std::make_shared<DataTypeQuantileState>();
    auto function = create_aggregate_function_quantile_state_union(
            "quantile_union", {type}, type, false,
            {.is_window_function = is_window, .column_names = {}});
    std::unique_ptr<char[]> memory(new char[function->size_of_data()]);
    auto* place = memory.get();
    function->create(place);
    Defer destroy([&] { function->destroy(place); });
    Arena arena;
    auto input = ColumnQuantileState::create();
    QuantileState seed(10000);
    for (size_t i = 0; i < count; ++i) {
        seed.add_value(20);
    }
    input->insert_value(seed);
    QuantileState next(2048);
    next.add_value(30);
    input->insert_value(next);
    const IColumn* columns[] = {input.get()};

    auto results = ColumnQuantileState::create();
    function->insert_result_into(place, *results);
    function->add(place, columns, 0, arena);
    function->insert_result_into(place, *results);
    function->reset(place);
    function->insert_result_into(place, *results);
    function->reset(place);
    function->insert_result_into(place, *results);
    function->add(place, columns, 1, arena);
    function->insert_result_into(place, *results);

    ASSERT_EQ(5, results->size());
    for (double percentile : {0.0, 0.5, 1.0}) {
        EXPECT_TRUE(std::isnan(results->get_element(0).get_value_by_percentile(percentile)));
        EXPECT_DOUBLE_EQ(20, results->get_element(1).get_value_by_percentile(percentile));
        EXPECT_TRUE(std::isnan(results->get_element(2).get_value_by_percentile(percentile)));
        EXPECT_TRUE(std::isnan(results->get_element(3).get_value_by_percentile(percentile)));
        EXPECT_DOUBLE_EQ(30, results->get_element(4).get_value_by_percentile(percentile));
        EXPECT_DOUBLE_EQ(20, input->get_element(0).get_value_by_percentile(percentile));
    }
    EXPECT_EQ(sizeof(float) + 1, results->get_element(2).get_serialized_size());
    EXPECT_EQ(sizeof(float) + 1, results->get_element(3).get_serialized_size());
}

void append_explicit_frame(const AggregateFunctionPtr& function, AggregateDataPtr place,
                           const IColumn** columns, size_t row, size_t frame_size, Arena& arena,
                           ColumnQuantileState& results, QuantileState& accumulator) {
    for (size_t sample = 0; sample < frame_size; ++sample) {
        function->add(place, columns, row, arena);
    }
    ASSERT_EQ(EXPLICIT, accumulator._type);
    const size_t capacity = accumulator._explicit_data.capacity();
    ASSERT_GE(capacity, frame_size);
    function->insert_result_into(place, results);
    function->reset(place);
    EXPECT_EQ(EMPTY, accumulator._type);
    EXPECT_TRUE(accumulator._explicit_data.empty());
    // Bounded ROWS frames reset on every row; the warmed vector must survive each reset.
    EXPECT_EQ(capacity, accumulator._explicit_data.capacity());
    function->insert_result_into(place, results);
    function->add(place, columns, row, arena);
    EXPECT_EQ(capacity, accumulator._explicit_data.capacity());
    function->reset(place);
}

void expect_repeated_explicit_frames_reuse_capacity(size_t frame_size) {
    auto type = std::make_shared<DataTypeQuantileState>();
    auto function = create_aggregate_function_quantile_state_union(
            "quantile_union", {type}, type, false,
            {.is_window_function = true, .column_names = {}});
    std::unique_ptr<char[]> memory(new char[function->size_of_data()]);
    auto* place = memory.get();
    function->create(place);
    Defer destroy([&] { function->destroy(place); });
    Arena arena;
    auto input = ColumnQuantileState::create();
    for (double value : {20.0, 30.0}) {
        QuantileState state;
        state.add_value(value);
        input->insert_value(state);
    }
    const IColumn* columns[] = {input.get()};
    using Data = AggregateFunctionQuantileStateData<AggregateFunctionQuantileStateUnionOp>;
    auto& accumulator = reinterpret_cast<Data*>(place)->value;

    auto results = ColumnQuantileState::create();
    for (size_t frame = 0; frame < 3; ++frame) {
        append_explicit_frame(function, place, columns, frame % 2, frame_size, arena, *results,
                              accumulator);
    }
    ASSERT_EQ(6, results->size());
    for (size_t frame = 0; frame < 3; ++frame) {
        const double expected = frame % 2 == 0 ? 20 : 30;
        EXPECT_DOUBLE_EQ(expected, results->get_element(2 * frame).get_value_by_percentile(0.5));
        EXPECT_TRUE(std::isnan(results->get_element(2 * frame + 1).get_value_by_percentile(0.5)));
    }
}

} // namespace

TEST(AggregateFunctionQuantileStateTest, EmptyWindowAfterNonemptyWindow) {
    auto type = std::make_shared<DataTypeQuantileState>();
    auto function = create_aggregate_function_quantile_state_union(
            "quantile_union", {type}, type, false,
            {.is_window_function = true, .column_names = {}});
    std::unique_ptr<char[]> memory(new char[function->size_of_data()]);
    auto* place = memory.get();
    function->create(place);
    Defer destroy([&] { function->destroy(place); });
    Arena arena;
    auto input = ColumnQuantileState::create();
    QuantileState state;
    state.add_value(20);
    input->insert_value(state);
    const IColumn* columns[] = {input.get()};

    auto results = ColumnQuantileState::create();
    function->insert_result_into(place, *results);
    function->add(place, columns, 0, arena);
    function->insert_result_into(place, *results);
    function->reset(place);
    function->insert_result_into(place, *results);

    ASSERT_EQ(3, results->size());
    for (double percentile : {0.0, 0.5, 1.0}) {
        EXPECT_TRUE(std::isnan(results->get_element(0).get_value_by_percentile(percentile)));
        EXPECT_DOUBLE_EQ(20, results->get_element(1).get_value_by_percentile(percentile));
        EXPECT_TRUE(std::isnan(results->get_element(2).get_value_by_percentile(percentile)));
    }
}

TEST(AggregateFunctionQuantileStateTest, ResetClearsAllRepresentations) {
    for (bool is_window : {false, true}) {
        for (size_t count : {size_t(1), size_t(3), size_t(QUANTILE_STATE_EXPLICIT_NUM + 1)}) {
            expect_reset_quantile_state(count, is_window);
        }
    }
}

TEST(AggregateFunctionQuantileStateTest, RepeatedExplicitFramesReuseCapacity) {
    for (size_t frame_size : {size_t(2), size_t(3), size_t(QUANTILE_STATE_EXPLICIT_NUM)}) {
        expect_repeated_explicit_frames_reuse_capacity(frame_size);
    }
}

TEST(AggregateFunctionQuantileStateTest, NullOnlyWindowAfterResetIsEmpty) {
    auto type = std::make_shared<DataTypeQuantileState>();
    auto function = create_aggregate_function_quantile_state_union(
            "quantile_union", {make_nullable(type)}, type, false,
            {.is_window_function = true, .column_names = {}});
    std::unique_ptr<char[]> memory(new char[function->size_of_data()]);
    auto* place = memory.get();
    function->create(place);
    Defer destroy([&] { function->destroy(place); });
    Arena arena;
    auto nested = ColumnQuantileState::create();
    QuantileState state;
    state.add_value(20);
    nested->insert_value(state);
    nested->insert_default();
    auto null_map = ColumnUInt8::create();
    null_map->insert_value(0);
    null_map->insert_value(1);
    auto input = ColumnNullable::create(std::move(nested), std::move(null_map));
    const IColumn* columns[] = {input.get()};

    auto results = ColumnQuantileState::create();
    function->add(place, columns, 0, arena);
    function->insert_result_into(place, *results);
    function->reset(place);
    function->add(place, columns, 1, arena);
    function->insert_result_into(place, *results);

    ASSERT_EQ(2, results->size());
    for (double percentile : {0.0, 0.5, 1.0}) {
        EXPECT_DOUBLE_EQ(20, results->get_element(0).get_value_by_percentile(percentile));
        EXPECT_TRUE(std::isnan(results->get_element(1).get_value_by_percentile(percentile)));
    }
}

TEST(AggregateFunctionQuantileStateTest, GrowingWindowKeepsResultsCompact) {
    auto type = std::make_shared<DataTypeQuantileState>();
    auto function = create_aggregate_function_quantile_state_union(
            "quantile_union", {type}, type, false,
            {.is_window_function = true, .column_names = {}});
    std::unique_ptr<char[]> memory(new char[function->size_of_data()]);
    auto* place = memory.get();
    function->create(place);
    Defer destroy([&] { function->destroy(place); });
    Arena arena;
    auto input = ColumnQuantileState::create();
    QuantileState seed(10000);
    constexpr size_t seed_count = 4096;
    for (size_t i = 0; i < seed_count; ++i) {
        seed.add_value(10);
    }
    input->insert_value(std::move(seed));
    const IColumn* columns[] = {input.get()};
    function->add(place, columns, 0, arena);
    input->clear();
    using Data = AggregateFunctionQuantileStateData<AggregateFunctionQuantileStateUnionOp>;
    auto& accumulator = reinterpret_cast<Data*>(place)->value;
    auto* original = accumulator._tdigest_ptr.get();
    const size_t initial_unprocessed = accumulator._mutable_tdigest().unprocessed().size();
    auto results = ColumnQuantileState::create();
    constexpr size_t result_count = 12000;
    results->reserve(result_count);
    bool reused_accumulator = true;
    size_t unprocessed_centroids = 0;
    for (size_t row = 0; row < result_count; ++row) {
        QuantileState value;
        value.add_value(20 + row);
        input->clear();
        input->insert_value(std::move(value));
        function->add(place, columns, 0, arena);
        unprocessed_centroids += accumulator._mutable_tdigest().unprocessed().size();
        function->insert_result_into(place, *results);
        reused_accumulator &= accumulator._tdigest_ptr.get() == original;
    }
    EXPECT_TRUE(reused_accumulator);
    // Each sample should enter result-insertion sorting only once across the window.
    EXPECT_EQ(initial_unprocessed + result_count, unprocessed_centroids);
    RecordProperty("unprocessed_centroids", std::to_string(unprocessed_centroids));
    EXPECT_NE(accumulator._tdigest_ptr, results->get_element(result_count - 1)._tdigest_ptr);
    // Inspect actual capacities independently of the column's approximate accounting.
    size_t digest_bytes = 0;
    for (auto& result : results->get_data()) {
        auto& digest = result._mutable_tdigest();
        ASSERT_EQ(0, digest._unprocessed.capacity());
        ASSERT_EQ(digest._processed.size(), digest._processed.capacity());
        ASSERT_EQ(digest._cumulative.size(), digest._cumulative.capacity());
        digest_bytes +=
                (digest._processed.capacity() + digest._unprocessed.capacity()) * sizeof(Centroid) +
                digest._cumulative.capacity() * sizeof(Weight);
    }
    RecordProperty("retained_digest_bytes", std::to_string(digest_bytes));
    EXPECT_LT(digest_bytes, result_count * 160 * 1024);
    for (size_t row : {size_t(0), result_count / 2, result_count - 1}) {
        auto& result = results->get_element(row);
        EXPECT_EQ(0, result._mutable_tdigest().unprocessed().capacity());
        EXPECT_EQ(10, result.get_value_by_percentile(0));
        EXPECT_EQ(20 + row, result.get_value_by_percentile(1));
        std::vector<double> values(seed_count, 10);
        for (size_t i = 0; i <= row; ++i) {
            values.push_back(20 + i);
        }
        const std::vector<double> quantiles {0.5, 0.9, 0.99};
        const auto expected = expected_quantiles(values, quantiles, 10000);
        for (size_t i = 0; i < quantiles.size(); ++i) {
            EXPECT_NEAR(expected[i], result.get_value_by_percentile(quantiles[i]), 1.0);
        }
    }
    // The accumulator keeps its write buffer for later rows instead of shrinking like a
    // result copy: its unprocessed vector still has spare capacity after the window, while
    // the retained result digests above are sized exactly.
    EXPECT_GT(accumulator._mutable_tdigest().unprocessed().capacity(), 0);
    EXPECT_FALSE(accumulator._mutable_tdigest().haveUnprocessed());
}

class AggregateFunctionQuantileStateRangeTest : public testing::TestWithParam<bool> {};

TEST_P(AggregateFunctionQuantileStateRangeTest, RangeResultsShareOneSavedDigest) {
    const bool is_window = GetParam();
    auto type = std::make_shared<DataTypeQuantileState>();
    auto function = create_aggregate_function_quantile_state_union(
            "quantile_union", {type}, type, false,
            {.is_window_function = is_window, .column_names = {}});
    std::unique_ptr<char[]> memory(new char[function->size_of_data()]);
    auto* place = memory.get();
    function->create(place);
    Defer destroy([&] { function->destroy(place); });
    Arena arena;
    auto input = ColumnQuantileState::create();
    QuantileState state(10000);
    for (int i = 0; i < 4096; ++i) {
        state.add_value(10);
    }
    input->insert_value(state);
    const IColumn* columns[] = {input.get()};
    function->add(place, columns, 0, arena);
    auto results = ColumnQuantileState::create();
    results->insert_many_defaults(3);
    function->insert_result_into_range(place, *results, 3, 12003);
    function->insert_result_into_range(place, *results, 12003, 12003);
    ASSERT_EQ(12003, results->size());
    const size_t column_bytes = results->allocated_bytes();
    const auto& first = results->get_element(3);
    for (size_t row = 3; row < results->size(); ++row) {
        EXPECT_EQ(first._tdigest_ptr, results->get_element(row)._tdigest_ptr);
    }
    if (is_window) {
        EXPECT_NE(state._tdigest_ptr, first._tdigest_ptr);
    } else {
        EXPECT_EQ(state._tdigest_ptr, first._tdigest_ptr);
    }
    auto modified = first;
    modified.add_value(110);
    EXPECT_EQ(110, modified.get_value_by_percentile(1));
    EXPECT_EQ(10, first.get_value_by_percentile(1));
    EXPECT_EQ(10, state.get_value_by_percentile(1));
    EXPECT_EQ(column_bytes, results->allocated_bytes());
}

INSTANTIATE_TEST_SUITE_P(AggregateAndWindow, AggregateFunctionQuantileStateRangeTest,
                         testing::Bool());

} // namespace doris
