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

#include "core/column/column_complex.h"
#include "core/data_type/data_type_quantilestate.h"
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

} // namespace

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
    EXPECT_GE(accumulator._mutable_tdigest().unprocessed().capacity(), 80001);
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
