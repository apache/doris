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

#include <cstdint>
#include <string>

#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/string_buffer.hpp"
#include "exprs/aggregate/aggregate_function_topn.h"

namespace doris {
namespace {

template <PrimitiveType T>
AggregateFunctionTopNData<T> round_trip(const AggregateFunctionTopNData<T>& state) {
    auto column = ColumnString::create();
    BufferWritable writer(*column);
    state.write(writer);
    writer.commit();
    BufferReadable reader(column->get_data_at(0));
    AggregateFunctionTopNData<T> result;
    result.read(reader);
    return result;
}

class AggregateFunctionTopNUnlimitedTest : public testing::TestWithParam<int> {};

TEST_P(AggregateFunctionTopNUnlimitedTest, SerializeAndMergeStrings) {
    AggregateFunctionTopNData<TYPE_STRING> lhs;
    AggregateFunctionTopNData<TYPE_STRING> rhs;
    lhs.set_paramenters(1, GetParam());
    rhs.set_paramenters(1, GetParam());
    // The global winner is not the most frequent value in either partial state.
    lhs.add(std::string("a"), 3);
    lhs.add(std::string("winner"), 2);
    rhs.add(std::string("b"), 3);
    rhs.add(std::string("winner"), 2);

    auto partial = round_trip(lhs);
    EXPECT_EQ(partial.counter_map, lhs.counter_map);
    AggregateFunctionTopNData<TYPE_STRING> merged;
    merged.merge(partial);
    merged = round_trip(merged);
    merged.merge(round_trip(rhs));
    merged = round_trip(merged);

    ASSERT_EQ(merged.counter_map.size(), 3);
    EXPECT_EQ(merged.counter_map.at("a"), 3);
    EXPECT_EQ(merged.counter_map.at("b"), 3);
    EXPECT_EQ(merged.counter_map.at("winner"), 4);
    EXPECT_EQ(merged.get(), R"({"winner":4})");

    AggregateFunctionTopNData<TYPE_STRING> empty;
    merged.merge(round_trip(empty));
    EXPECT_EQ(merged.get(), R"({"winner":4})");

    merged.reset();
    EXPECT_EQ(round_trip(merged).get(), "{}");
    merged.set_paramenters(1, GetParam());
    merged.add(std::string("new"), 7);
    EXPECT_EQ(round_trip(merged).get(), R"({"new":7})");
}

TEST_P(AggregateFunctionTopNUnlimitedTest, SerializeAndMergeWeightedIntegers) {
    AggregateFunctionTopNData<TYPE_INT> lhs;
    AggregateFunctionTopNData<TYPE_INT> rhs;
    lhs.set_paramenters(2, GetParam());
    rhs.set_paramenters(2, GetParam());
    lhs.add(1, 10);
    lhs.add(2, 7);
    lhs.add(3, 6);
    rhs.add(4, 11);
    rhs.add(5, 8);
    rhs.add(3, 6);

    AggregateFunctionTopNData<TYPE_INT> merged;
    merged.merge(round_trip(lhs));
    merged.merge(round_trip(rhs));
    merged = round_trip(merged);
    ASSERT_EQ(merged.counter_map.size(), 5);
    EXPECT_EQ(merged.counter_map.at(3), 12);
    auto result = ColumnInt32::create();
    merged.insert_result_into(*result);
    ASSERT_EQ(result->size(), 2);
    EXPECT_EQ(result->get_element(0), 3);
    EXPECT_EQ(result->get_element(1), 4);
}

INSTANTIATE_TEST_SUITE_P(NonPositiveRates, AggregateFunctionTopNUnlimitedTest,
                         testing::Values(0, -1, INT32_MIN));

TEST(AggregateFunctionTopNTest, PositiveRateStillLimitsSerializedCandidates) {
    AggregateFunctionTopNData<TYPE_INT> state;
    state.set_paramenters(1, 2);
    state.add(1, 3);
    state.add(2, 2);
    state.add(3, 1);
    auto partial = round_trip(state);
    ASSERT_EQ(partial.counter_map.size(), 2);
    EXPECT_EQ(partial.counter_map.at(1), 3);
    EXPECT_EQ(partial.counter_map.at(2), 2);

    state.set_paramenters(1);
    EXPECT_EQ(round_trip(state).counter_map, state.counter_map);
}

} // namespace
} // namespace doris
