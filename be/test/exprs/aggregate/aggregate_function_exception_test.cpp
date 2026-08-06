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

#include <vector>

#include "core/arena.h"
#include "exprs/aggregate/aggregate_function.h"

namespace doris {

struct TrackingAggregateState {
    TrackingAggregateState() { ++construct_count; }
    ~TrackingAggregateState() { ++destroy_count; }

    static void reset_counters() {
        construct_count = 0;
        destroy_count = 0;
    }

    static int construct_count;
    static int destroy_count;
};

int TrackingAggregateState::construct_count = 0;
int TrackingAggregateState::destroy_count = 0;

class ThrowOnDeserializeAggregateFunction final
        : public IAggregateFunctionDataHelper<TrackingAggregateState,
                                              ThrowOnDeserializeAggregateFunction> {
public:
    ThrowOnDeserializeAggregateFunction()
            : IAggregateFunctionDataHelper<TrackingAggregateState,
                                           ThrowOnDeserializeAggregateFunction>(
                      DataTypes {std::make_shared<DataTypeString>()}) {}

    String get_name() const override { return "throw_on_deserialize"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeString>(); }

    void add(AggregateDataPtr, const IColumn**, ssize_t, Arena&) const override {}

    void merge(AggregateDataPtr, ConstAggregateDataPtr, Arena&) const override {}

    void serialize(ConstAggregateDataPtr, BufferWritable& buf) const override {
        String payload;
        buf.write_binary(payload);
    }

    void deserialize(AggregateDataPtr, BufferReadable& buf, Arena&) const override {
        String payload;
        buf.read_binary(payload);
        if (payload == "throw") {
            throw Exception(ErrorCode::INTERNAL_ERROR, "mock deserialize failure");
        }
    }

    void insert_result_into(ConstAggregateDataPtr, IColumn&) const override {}
};

class AggregateFunctionExceptionTest : public testing::Test {
protected:
    void SetUp() override { TrackingAggregateState::reset_counters(); }

    MutableColumnPtr make_column(std::initializer_list<String> payloads) {
        auto column = ColumnString::create();
        VectorBufferWriter writer(*column);
        for (const auto& payload : payloads) {
            writer.write_binary(payload);
            writer.commit();
        }
        return column;
    }

    ThrowOnDeserializeAggregateFunction function;
    Arena arena;
};

TEST_F(AggregateFunctionExceptionTest, DeserializeVecDestroysCurrentStateOnFailure) {
    auto column = make_column({"ok", "throw"});
    std::vector<char> states(function.size_of_data() * 2);

    bool thrown = false;
    try {
        function.deserialize_vec(states.data(), static_cast<ColumnString*>(column.get()), arena, 2);
    } catch (const Exception&) {
        thrown = true;
    }

    EXPECT_TRUE(thrown);
    if (!thrown) {
        function.destroy_vec(states.data(), 2);
    }
    EXPECT_EQ(TrackingAggregateState::construct_count, 2);
    EXPECT_EQ(TrackingAggregateState::destroy_count, 2);
}

TEST_F(AggregateFunctionExceptionTest, DeserializeAndMergeVecDestroysRhsStateOnFailure) {
    auto column = make_column({"throw"});
    std::vector<char> place_storage(function.size_of_data());
    std::vector<char> rhs_storage(function.size_of_data());
    auto* place = place_storage.data();
    function.create(place);

    std::array<AggregateDataPtr, 1> places {place};
    const auto destroy_count_before_call = TrackingAggregateState::destroy_count;
    bool thrown = false;
    try {
        function.deserialize_and_merge_vec(places.data(), 0, rhs_storage.data(), column.get(),
                                           arena, 1);
    } catch (const Exception&) {
        thrown = true;
    }

    EXPECT_TRUE(thrown);
    EXPECT_EQ(TrackingAggregateState::destroy_count - destroy_count_before_call, 1);

    function.destroy(place);
    EXPECT_EQ(TrackingAggregateState::construct_count, TrackingAggregateState::destroy_count);
}

TEST_F(AggregateFunctionExceptionTest,
       DeserializeAndMergeVecSelectedDestroysAllCreatedRhsStatesOnFailure) {
    auto column = make_column({"skip", "throw"});
    std::vector<char> place_storage(function.size_of_data());
    std::vector<char> rhs_storage(function.size_of_data() * 2);
    auto* place = place_storage.data();
    function.create(place);

    std::array<AggregateDataPtr, 2> places {nullptr, place};
    const auto destroy_count_before_call = TrackingAggregateState::destroy_count;
    bool thrown = false;
    try {
        function.deserialize_and_merge_vec_selected(places.data(), 0, rhs_storage.data(),
                                                    column.get(), arena, 2);
    } catch (const Exception&) {
        thrown = true;
    }

    EXPECT_TRUE(thrown);
    EXPECT_EQ(TrackingAggregateState::destroy_count - destroy_count_before_call, 2);

    function.destroy(place);
    EXPECT_EQ(TrackingAggregateState::construct_count, TrackingAggregateState::destroy_count);
}

} // namespace doris