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

#include <DataSketches/hll.hpp>

#include "core/column/column.h"
#include "core/column/column_string.h"
#include "exec/common/hash_table/hash.h"
#include "exprs/aggregate/aggregate_function_datasketches_hll_union_agg.h"

namespace doris {

class AggregateFunctionDataSketchesHllUnionAggTest : public ::testing::Test {
protected:
    void SetUp() override { arena = std::make_unique<Arena>(); }

    void TearDown() override { arena.reset(); }

    std::unique_ptr<Arena> arena;
};

TEST_F(AggregateFunctionDataSketchesHllUnionAggTest, testBasicUnion) {
    // Create test: union multiple hll sketches and get correct cardinality estimate
    DataTypePtr input_type = std::make_shared<DataTypeString>();
    DataTypes argument_types = {input_type};

    auto agg_func = std::make_shared<AggregateFunctionDataSketchesHllUnionAgg<
            TYPE_STRING, AggregateFunctionHllSketchData<TYPE_STRING>>>(argument_types);

    // Create 2 different hll sketches, each with 100 unique values
    datasketches::hll_sketch sketch1(8); // lg_k=8
    for (int i = 0; i < 100; i++) {
        sketch1.update(i);
    }

    datasketches::hll_sketch sketch2(8);
    for (int i = 50; i < 150; i++) {
        sketch2.update(i);
    }

    // Serialize both sketches into string column
    auto column_string = ColumnString::create();
    const auto ser1 = sketch1.serialize_compact();
    column_string->insert_data((const char*)(ser1.data()), ser1.size());
    const auto ser2 = sketch2.serialize_compact();
    column_string->insert_data((const char*)(ser2.data()), ser2.size());

    // Create aggregate data place
    AggregateDataPtr place = arena->aligned_alloc(
            agg_func->size_of_data(), agg_func->align_of_data());
    agg_func->create(place);

    // Add both rows
    const IColumn* columns[1] = {column_string.get()};
    agg_func->add(place, columns, 0, *arena);
    agg_func->add(place, columns, 1, *arena);

    // Get result
    ColumnInt64 result_column;
    agg_func->insert_result_into(place, result_column);

    // After union, we expect ~150 unique values (0-149)
    int64_t estimate = result_column.get_data()[0];
    EXPECT_GE(estimate, 130); // HLL estimate has some error, allow 13% error
    EXPECT_LE(estimate, 170);

    agg_func->destroy(place);
}

TEST_F(AggregateFunctionDataSketchesHllUnionAggTest, testMergeTwoAggStates) {
    DataTypePtr input_type = std::make_shared<DataTypeString>();
    DataTypes argument_types = {input_type};

    using AggFunc =
            AggregateFunctionDataSketchesHllUnionAgg<TYPE_STRING,
                                                     AggregateFunctionHllSketchData<TYPE_STRING>>;
    auto agg_func = std::make_shared<AggFunc>(argument_types);

    // Create two separate aggregate states
    AggregateDataPtr place1 =
            arena->aligned_alloc(agg_func->size_of_data(), agg_func->align_of_data());
    agg_func->create(place1);

    AggregateDataPtr place2 =
            arena->aligned_alloc(agg_func->size_of_data(), agg_func->align_of_data());
    agg_func->create(place2);

    // Add different data to each state
    datasketches::hll_sketch sketch1(8);
    for (int i = 0; i < 100; i++) sketch1.update(i);
    const auto ser1 = sketch1.serialize_compact();

    datasketches::hll_sketch sketch2(8);
    for (int i = 100; i < 200; i++) sketch2.update(i);
    const auto ser2 = sketch2.serialize_compact();

    auto column_string = ColumnString::create();
    column_string->insert_data((const char*)(ser1.data()), ser1.size());
    column_string->insert_data((const char*)(ser2.data()), ser2.size());

    const IColumn* columns[1] = {column_string.get()};
    agg_func->add(place1, columns, 0, *arena);
    agg_func->add(place2, columns, 1, *arena);

    // Merge second state into first
    agg_func->merge(place1, place2, *arena);

    // Get result
    ColumnInt64 result;
    agg_func->insert_result_into(place1, result);
    int64_t estimate = result.get_data()[0];

    EXPECT_GE(estimate, 170);
    EXPECT_LE(estimate, 230); // 200 unique values expected

    agg_func->destroy(place1);
    agg_func->destroy(place2);
}

TEST_F(AggregateFunctionDataSketchesHllUnionAggTest, testEmptyState) {
    DataTypePtr input_type = std::make_shared<DataTypeString>();
    DataTypes argument_types = {input_type};

    auto agg_func = std::make_shared<AggregateFunctionDataSketchesHllUnionAgg<
            TYPE_STRING, AggregateFunctionHllSketchData<TYPE_STRING>>>(argument_types);

    AggregateDataPtr place =
            arena->aligned_alloc(agg_func->size_of_data(), agg_func->align_of_data());
    agg_func->create(place);

    ColumnInt64 result;
    agg_func->insert_result_into(place, result);

    EXPECT_EQ(result.get_data()[0], 0);

    agg_func->destroy(place);
}

TEST_F(AggregateFunctionDataSketchesHllUnionAggTest, testSerializeDeserialize) {
    DataTypePtr input_type = std::make_shared<DataTypeString>();
    DataTypes argument_types = {input_type};

    auto agg_func = std::make_shared<AggregateFunctionDataSketchesHllUnionAgg<
            TYPE_STRING, AggregateFunctionHllSketchData<TYPE_STRING>>>(argument_types);

    // Add some data
    datasketches::hll_sketch sketch(8);
    for (int i = 0; i < 100; i++) sketch.update(i);
    const auto ser = sketch.serialize_compact();

    auto column_string = ColumnString::create();
    column_string->insert_data((const char*)(ser.data()), ser.size());

    AggregateDataPtr place =
            arena->aligned_alloc(agg_func->size_of_data(), agg_func->align_of_data());
    agg_func->create(place);

    const IColumn* columns[1] = {column_string.get()};
    agg_func->add(place, columns, 0, *arena);

    // Serialize
    auto buffer = ColumnString::create();
    BufferWritable w(*buffer);
    agg_func->serialize(place, w);
    w.commit();

    // Deserialize into new state
    AggregateDataPtr new_place =
            arena->aligned_alloc(agg_func->size_of_data(), agg_func->align_of_data());
    agg_func->create(new_place);

    BufferReadable r(buffer->get_data_at(0));
    agg_func->deserialize(new_place, r, *arena);

    // Compare results
    ColumnInt64 result1, result2;
    agg_func->insert_result_into(place, result1);
    agg_func->insert_result_into(new_place, result2);

    EXPECT_EQ(result1.get_data()[0], result2.get_data()[0]);

    agg_func->destroy(place);
    agg_func->destroy(new_place);
}

TEST_F(AggregateFunctionDataSketchesHllUnionAggTest, testReset) {
    DataTypePtr input_type = std::make_shared<DataTypeString>();
    DataTypes argument_types = {input_type};

    auto agg_func = std::make_shared<AggregateFunctionDataSketchesHllUnionAgg<
            TYPE_STRING, AggregateFunctionHllSketchData<TYPE_STRING>>>(argument_types);

    datasketches::hll_sketch sketch(8);
    for (int i = 0; i < 100; i++) sketch.update(i);
    auto ser = sketch.serialize_compact();

    auto column_string = ColumnString::create();
    column_string->insert_data((const char*)(ser.data()), ser.size());

    AggregateDataPtr place =
            arena->aligned_alloc(agg_func->size_of_data(), agg_func->align_of_data());
    agg_func->create(place);

    const IColumn* columns[1] = {column_string.get()};
    agg_func->add(place, columns, 0, *arena);

    // Reset
    agg_func->reset(place);

    ColumnInt64 result;
    agg_func->insert_result_into(place, result);
    EXPECT_EQ(result.get_data()[0], 0);

    agg_func->destroy(place);
}

} // namespace doris