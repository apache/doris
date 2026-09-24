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

#include "core/value/quantile_state.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "core/column/column_complex.h"
#include "exprs/aggregate/aggregate_function_quantile_state.h"
#include "gtest/gtest_pred_impl.h"
#include "util/tdigest.h"

namespace doris {

namespace {

QuantileState make_digest_state() {
    QuantileState state;
    for (int i = 0; i < 4096; ++i) {
        state.add_value(10);
    }
    return state;
}

std::string serialize_state(const QuantileState& state) {
    std::string bytes(state.get_serialized_size(), '\0');
    state.serialize(reinterpret_cast<uint8_t*>(bytes.data()));
    return bytes;
}

} // namespace

TEST(QuantileStateTest, CopyDoesNotModifyInput) {
    auto source = make_digest_state();
    const auto before = serialize_state(source);
    auto copy = source;
    copy.add_value(1000);
    EXPECT_EQ(before, serialize_state(source));
    EXPECT_DOUBLE_EQ(10, source.get_value_by_percentile(1));
    EXPECT_DOUBLE_EQ(1000, copy.get_value_by_percentile(1));
}

TEST(QuantileStateTest, MergeDoesNotModifyInput) {
    for (int initial_size = 0; initial_size < 3; ++initial_size) {
        auto source = make_digest_state();
        const auto before = serialize_state(source);
        QuantileState target;
        for (int i = 0; i < initial_size; ++i) {
            target.add_value(1000);
        }
        target.merge(source);
        target.add_value(2000);
        EXPECT_EQ(before, serialize_state(source)) << initial_size;
        EXPECT_DOUBLE_EQ(10, source.get_value_by_percentile(1));
        EXPECT_DOUBLE_EQ(2000, target.get_value_by_percentile(1));
    }
}

TEST(QuantileStateTest, PercentileDoesNotModifyInput) {
    const auto source = make_digest_state();
    const auto before = serialize_state(source);
    EXPECT_DOUBLE_EQ(10, source.get_value_by_percentile(0.5));
    EXPECT_EQ(before, serialize_state(source));
}

TEST(QuantileStateTest, AssignmentAndColumnCopiesDoNotModifyInput) {
    auto source = make_digest_state();
    const auto before = serialize_state(source);
    QuantileState assigned;
    assigned = source;
    assigned.add_value(1000);
    EXPECT_EQ(before, serialize_state(source));

    auto column = ColumnQuantileState::create();
    column->get_data().push_back(source);
    auto cloned = column->clone_resized(1);
    auto& copied_state = assert_cast<ColumnQuantileState&>(*cloned).get_data()[0];
    copied_state.add_value(2000);
    EXPECT_EQ(before, serialize_state(column->get_data()[0]));
    EXPECT_DOUBLE_EQ(2000, copied_state.get_value_by_percentile(1));
}

TEST(QuantileStateTest, AggregatesAndResultsDoNotAliasInput) {
    auto source = make_digest_state();
    const auto before = serialize_state(source);
    AggregateFunctionQuantileStateData<AggregateFunctionQuantileStateUnionOp> first;
    AggregateFunctionQuantileStateData<AggregateFunctionQuantileStateUnionOp> second;
    first.add(source);
    second.add(source);
    QuantileState extra;
    extra.add_value(1000);
    first.add(extra);
    auto result = first.get();
    const auto result_before = serialize_state(result);
    extra.add_value(2000);
    first.add(extra);
    EXPECT_EQ(before, serialize_state(source));
    EXPECT_EQ(before, serialize_state(second.get()));
    EXPECT_EQ(result_before, serialize_state(result));
    EXPECT_DOUBLE_EQ(1000, result.get_value_by_percentile(1));
    EXPECT_DOUBLE_EQ(2000, first.get().get_value_by_percentile(1));
}

TEST(QuantileStateTest, SelfMergeDuplicatesExplicitAndDigestValues) {
    QuantileState explicit_state;
    explicit_state.add_value(10);
    explicit_state.add_value(20);
    explicit_state.merge(explicit_state);
    EXPECT_EQ(4, explicit_state._explicit_data.size());
    EXPECT_DOUBLE_EQ(15, explicit_state.get_value_by_percentile(0.5));

    auto digest = make_digest_state();
    const auto before = digest._tdigest_ptr->total_weight();
    digest.merge(digest);
    EXPECT_EQ(before * 2, digest._tdigest_ptr->total_weight());
    EXPECT_DOUBLE_EQ(10, digest.get_value_by_percentile(0.5));
}

TEST(QuantileStateTest, merge) {
    QuantileState empty;
    EXPECT_EQ(EMPTY, empty._type);
    empty.add_value(1);
    EXPECT_EQ(SINGLE, empty._type);
    empty.add_value(2);
    empty.add_value(3);
    empty.add_value(4);
    empty.add_value(5);
    EXPECT_EQ(1, empty.get_value_by_percentile(0));
    EXPECT_EQ(3, empty.get_value_by_percentile(0.5));
    EXPECT_EQ(5, empty.get_value_by_percentile(1));

    QuantileState another;
    another.add_value(6);
    another.add_value(7);
    another.add_value(8);
    another.add_value(9);
    another.add_value(10);
    EXPECT_EQ(6, another.get_value_by_percentile(0));
    EXPECT_EQ(8, another.get_value_by_percentile(0.5));
    EXPECT_EQ(10, another.get_value_by_percentile(1));

    another.merge(empty);
    EXPECT_EQ(1, another.get_value_by_percentile(0));
    EXPECT_EQ(5.5, another.get_value_by_percentile(0.5));
    EXPECT_EQ(10, another.get_value_by_percentile(1));
}

} // namespace doris
