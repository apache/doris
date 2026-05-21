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

#include "agent/be_exec_version_manager.h"
#include "common/status.h"
#include "core/column/column.h"
#include "core/column/column_string.h"
#include "core/column/column_varbinary.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_varbinary.h"
#include "exec/common/hash_table/hash.h"
#include "exprs/aggregate/aggregate_function_datasketches_hll_union_agg.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"

namespace doris {

void register_aggregate_function_datasketches_HLL_union_agg(
        AggregateFunctionSimpleFactory& factory);

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
    AggregateDataPtr place =
            arena->aligned_alloc(agg_func->size_of_data(), agg_func->align_of_data());
    agg_func->create(place);

    // Add both rows
    const IColumn* columns[1] = {column_string.get()};
    agg_func->add(place, columns, 0, *arena);
    agg_func->add(place, columns, 1, *arena);

    // Get result
    ColumnFloat64 result_column;
    agg_func->insert_result_into(place, result_column);

    double estimate = result_column.get_data()[0];
    EXPECT_GE(estimate, 130.0);
    EXPECT_LE(estimate, 170.0);

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

    EXPECT_GE(estimate, 170.0);
    EXPECT_LE(estimate, 230.0); // 200 unique values expected

    agg_func->destroy(place1);
    agg_func->destroy(place2);
}

TEST_F(AggregateFunctionDataSketchesHllUnionAggTest, testMergeEmptyStateDoesNotCrash) {
    DataTypePtr input_type = std::make_shared<DataTypeString>();
    DataTypes argument_types = {input_type};

    using AggFunc =
            AggregateFunctionDataSketchesHllUnionAgg<TYPE_STRING,
                                                     AggregateFunctionHllSketchData<TYPE_STRING>>;
    auto agg_func = std::make_shared<AggFunc>(argument_types);

    AggregateDataPtr place_with_data =
            arena->aligned_alloc(agg_func->size_of_data(), agg_func->align_of_data());
    agg_func->create(place_with_data);

    AggregateDataPtr empty_rhs_place =
            arena->aligned_alloc(agg_func->size_of_data(), agg_func->align_of_data());
    agg_func->create(empty_rhs_place);

    datasketches::hll_sketch sketch(8, datasketches::HLL_8);
    for (int i = 0; i < 100; ++i) {
        sketch.update(i);
    }
    const auto ser = sketch.serialize_compact();

    auto column_string = ColumnString::create();
    column_string->insert_data((const char*)ser.data(), ser.size());
    const IColumn* columns[1] = {column_string.get()};
    agg_func->add(place_with_data, columns, 0, *arena);

    // Covers the "all NULL" style path: rhs exists but never saw add().
    EXPECT_NO_THROW(agg_func->merge(place_with_data, empty_rhs_place, *arena));

    ColumnInt64 result;
    agg_func->insert_result_into(place_with_data, result);
    EXPECT_EQ(result.get_data()[0], static_cast<int64_t>(sketch.get_estimate()));

    // Empty string is invalid serialized sketch and should be rejected by add().
    // Merge-empty-state coverage is handled by the "never saw add()" path above.

    AggregateDataPtr empty_lhs_place =
            arena->aligned_alloc(agg_func->size_of_data(), agg_func->align_of_data());
    agg_func->create(empty_lhs_place);
    EXPECT_NO_THROW(agg_func->merge(empty_lhs_place, empty_rhs_place, *arena));

    ColumnInt64 empty_merge_result;
    agg_func->insert_result_into(empty_lhs_place, empty_merge_result);
    EXPECT_EQ(empty_merge_result.get_data()[0], 0);

    agg_func->destroy(place_with_data);
    agg_func->destroy(empty_rhs_place);
    agg_func->destroy(empty_lhs_place);
}

TEST_F(AggregateFunctionDataSketchesHllUnionAggTest, testEmptyState) {
    DataTypePtr input_type = std::make_shared<DataTypeString>();
    DataTypes argument_types = {input_type};

    auto agg_func = std::make_shared<AggregateFunctionDataSketchesHllUnionAgg<
            TYPE_STRING, AggregateFunctionHllSketchData<TYPE_STRING>>>(argument_types);

    AggregateDataPtr place =
            arena->aligned_alloc(agg_func->size_of_data(), agg_func->align_of_data());
    agg_func->create(place);

    ColumnFloat64 result;
    agg_func->insert_result_into(place, result);
    EXPECT_DOUBLE_EQ(result.get_data()[0], 0.0);

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

    EXPECT_DOUBLE_EQ(result1.get_data()[0], result2.get_data()[0]);

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
    EXPECT_DOUBLE_EQ(result.get_data()[0], 0.0);

    agg_func->destroy(place);
}

TEST_F(AggregateFunctionDataSketchesHllUnionAggTest, testResetThenAddReinitializesState) {
    DataTypePtr input_type = std::make_shared<DataTypeString>();
    DataTypes argument_types = {input_type};

    auto agg_func = std::make_shared<AggregateFunctionDataSketchesHllUnionAgg<
            TYPE_STRING, AggregateFunctionHllSketchData<TYPE_STRING>>>(argument_types);

    datasketches::hll_sketch sketch1(8, datasketches::HLL_8);
    for (int i = 0; i < 7; ++i) {
        sketch1.update(i);
    }
    const auto ser1 = sketch1.serialize_compact();

    datasketches::hll_sketch sketch2(8, datasketches::HLL_8);
    for (int i = 10; i < 17; ++i) {
        sketch2.update(i);
    }
    const auto ser2 = sketch2.serialize_compact();

    auto column_string = ColumnString::create();
    column_string->insert_data((const char*)ser1.data(), ser1.size());
    column_string->insert_data((const char*)ser2.data(), ser2.size());
    const IColumn* columns[1] = {column_string.get()};

    AggregateDataPtr place =
            arena->aligned_alloc(agg_func->size_of_data(), agg_func->align_of_data());
    agg_func->create(place);

    agg_func->add(place, columns, 0, *arena);
    agg_func->reset(place);
    agg_func->add(place, columns, 1, *arena);

    ColumnInt64 result;
    agg_func->insert_result_into(place, result);
    EXPECT_DOUBLE_EQ(result.get_data()[0], sketch2.get_estimate());

    agg_func->destroy(place);
}

TEST_F(AggregateFunctionDataSketchesHllUnionAggTest, testFactoryCreateAndAliases) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_datasketches_HLL_union_agg(factory);
    int be_version = BeExecVersionManager::get_newest_version();

    DataTypes argument_types = {std::make_shared<DataTypeString>()};

    auto fn_main =
            factory.get("datasketches_hll_union_agg", argument_types, nullptr, false, be_version);
    auto fn_alias_union_count =
            factory.get("ds_hll_union_count", argument_types, nullptr, false, be_version);
    auto fn_alias_cardinality =
            factory.get("ds_cardinality", argument_types, nullptr, false, be_version);

    ASSERT_NE(fn_main, nullptr);
    ASSERT_NE(fn_alias_union_count, nullptr);
    ASSERT_NE(fn_alias_cardinality, nullptr);

    datasketches::hll_sketch sketch(8, datasketches::HLL_8);
    for (int i = 0; i < 7; ++i) sketch.update(i);
    const auto ser = sketch.serialize_compact();

    auto column_string = ColumnString::create();
    column_string->insert_data((const char*)ser.data(), ser.size());
    const IColumn* columns[1] = {column_string.get()};

    auto run_and_get_result = [&](const AggregateFunctionPtr& fn) {
        AggregateDataPtr place = arena->aligned_alloc(fn->size_of_data(), fn->align_of_data());
        fn->create(place);
        fn->add(place, columns, 0, *arena);
        ColumnInt64 result;
        fn->insert_result_into(place, result);
        fn->destroy(place);
        return result.get_data()[0];
    };

    double expected = sketch.get_estimate();
    EXPECT_DOUBLE_EQ(run_and_get_result(fn_main), expected);
    EXPECT_DOUBLE_EQ(run_and_get_result(fn_alias_union_count), expected);
    EXPECT_DOUBLE_EQ(run_and_get_result(fn_alias_cardinality), expected);
}

TEST_F(AggregateFunctionDataSketchesHllUnionAggTest,
       testFactoryCreateForVarcharAndNullableAndUnsupportedType) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_datasketches_HLL_union_agg(factory);
    int be_version = BeExecVersionManager::get_newest_version();

    DataTypes varchar_types = {std::make_shared<DataTypeString>(-1, TYPE_VARCHAR)};
    auto fn_varchar =
            factory.get("datasketches_hll_union_agg", varchar_types, nullptr, false, be_version);
    auto fn_varchar_alias =
            factory.get("ds_cardinality", varchar_types, nullptr, false, be_version);
    ASSERT_NE(fn_varchar, nullptr);
    ASSERT_NE(fn_varchar_alias, nullptr);

    datasketches::hll_sketch sketch(8, datasketches::HLL_8);
    for (int i = 0; i < 7; ++i) {
        sketch.update(i);
    }
    const auto ser = sketch.serialize_compact();

    auto column_string = ColumnString::create();
    column_string->insert_data((const char*)ser.data(), ser.size());
    const IColumn* columns[1] = {column_string.get()};

    auto run_and_get_result = [&](const AggregateFunctionPtr& fn) {
        AggregateDataPtr place = arena->aligned_alloc(fn->size_of_data(), fn->align_of_data());
        fn->create(place);
        fn->add(place, columns, 0, *arena);
        ColumnInt64 result;
        fn->insert_result_into(place, result);
        fn->destroy(place);
        return result.get_data()[0];
    };

    double expected = sketch.get_estimate();
    EXPECT_DOUBLE_EQ(run_and_get_result(fn_varchar), expected);
    EXPECT_DOUBLE_EQ(run_and_get_result(fn_varchar_alias), expected);

    DataTypes nullable_varchar_types = {
            make_nullable(std::make_shared<DataTypeString>(-1, TYPE_VARCHAR))};
    auto fn_nullable_varchar = factory.get("datasketches_hll_union_agg", nullable_varchar_types,
                                           nullptr, false, be_version);
    ASSERT_NE(fn_nullable_varchar, nullptr);

    DataTypes unsupported_types = {std::make_shared<DataTypeInt32>()};
    auto fn_unsupported = factory.get("datasketches_hll_union_agg", unsupported_types, nullptr,
                                      false, be_version);
    EXPECT_EQ(fn_unsupported, nullptr);
}

TEST_F(AggregateFunctionDataSketchesHllUnionAggTest, testLowLgKSketchDoesNotReportCorruption) {
    DataTypes argument_types = {std::make_shared<DataTypeString>()};
    auto agg_func = std::make_shared<AggregateFunctionDataSketchesHllUnionAgg<
            TYPE_STRING, AggregateFunctionHllSketchData<TYPE_STRING>>>(argument_types);

    datasketches::hll_sketch sketch(4, datasketches::HLL_8);
    for (int i = 0; i < 100; ++i) {
        sketch.update(i);
    }
    const auto ser = sketch.serialize_compact();

    auto column_string = ColumnString::create();
    column_string->insert_data((const char*)ser.data(), ser.size());

    AggregateDataPtr place =
            arena->aligned_alloc(agg_func->size_of_data(), agg_func->align_of_data());
    agg_func->create(place);

    const IColumn* columns[1] = {column_string.get()};
    EXPECT_NO_THROW(agg_func->add(place, columns, 0, *arena));

    ColumnInt64 add_result;
    agg_func->insert_result_into(place, add_result);

    EXPECT_GE(add_result.get_data()[0], 50);
    EXPECT_LE(add_result.get_data()[0], 150);

    auto buf = ColumnString::create();
    BufferWritable w(*buf);
    StringRef d((const char*)ser.data(), ser.size());
    w.write_binary(d);
    w.commit();

    AggregateDataPtr place2 =
            arena->aligned_alloc(agg_func->size_of_data(), agg_func->align_of_data());
    agg_func->create(place2);

    BufferReadable r(buf->get_data_at(0));
    EXPECT_NO_THROW(agg_func->deserialize(place2, r, *arena));

    ColumnInt64 deserialize_result;
    agg_func->insert_result_into(place2, deserialize_result);
    EXPECT_EQ(deserialize_result.get_data()[0], add_result.get_data()[0]);

    agg_func->destroy(place);
    agg_func->destroy(place2);
}

TEST_F(AggregateFunctionDataSketchesHllUnionAggTest, testAddEmptyStringThrows) {
    DataTypes argument_types = {std::make_shared<DataTypeString>()};
    auto agg_func = std::make_shared<AggregateFunctionDataSketchesHllUnionAgg<
            TYPE_STRING, AggregateFunctionHllSketchData<TYPE_STRING>>>(argument_types);

    auto column_string = ColumnString::create();
    column_string->insert_data("", 0);

    AggregateDataPtr place =
            arena->aligned_alloc(agg_func->size_of_data(), agg_func->align_of_data());
    agg_func->create(place);

    const IColumn* columns[1] = {column_string.get()};

    try {
        agg_func->add(place, columns, 0, *arena);
        FAIL() << "Expected doris::Exception";
    } catch (const doris::Exception& e) {
        EXPECT_EQ(e.code(), doris::ErrorCode::CORRUPTION);
    }

    agg_func->destroy(place);
}

TEST_F(AggregateFunctionDataSketchesHllUnionAggTest, testResetOnEmptyState) {
    DataTypes argument_types = {std::make_shared<DataTypeString>()};
    auto agg_func = std::make_shared<AggregateFunctionDataSketchesHllUnionAgg<
            TYPE_STRING, AggregateFunctionHllSketchData<TYPE_STRING>>>(argument_types);

    AggregateDataPtr place =
            arena->aligned_alloc(agg_func->size_of_data(), agg_func->align_of_data());
    agg_func->create(place);

    EXPECT_NO_THROW(agg_func->reset(place)); // cover reset() branch when union_data is nullptr

    ColumnInt64 result;
    agg_func->insert_result_into(place, result);
    EXPECT_DOUBLE_EQ(result.get_data()[0], 0.0);

    agg_func->destroy(place);
}

TEST_F(AggregateFunctionDataSketchesHllUnionAggTest, testVarbinaryInput) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_datasketches_HLL_union_agg(factory);
    int be_version = BeExecVersionManager::get_newest_version();

    DataTypes argument_types = {std::make_shared<DataTypeVarbinary>()};
    auto fn = factory.get("datasketches_hll_union_agg", argument_types, nullptr, false, be_version);
    ASSERT_NE(fn, nullptr);

    datasketches::hll_sketch sketch(8, datasketches::HLL_8);
    for (int i = 20; i < 30; ++i) sketch.update(i);
    const auto ser = sketch.serialize_compact();

    auto column_varbinary = ColumnVarbinary::create();
    column_varbinary->insert_data((const char*)ser.data(), ser.size());

    const IColumn* columns[1] = {column_varbinary.get()};

    AggregateDataPtr place = arena->aligned_alloc(fn->size_of_data(), fn->align_of_data());
    fn->create(place);
    fn->add(place, columns, 0, *arena);

    ColumnInt64 result;
    fn->insert_result_into(place, result);
    EXPECT_DOUBLE_EQ(result.get_data()[0], sketch.get_estimate());

    fn->destroy(place);
}

TEST_F(AggregateFunctionDataSketchesHllUnionAggTest, testSerializeDeserializeEmptyState) {
    DataTypes argument_types = {std::make_shared<DataTypeString>()};
    auto agg_func = std::make_shared<AggregateFunctionDataSketchesHllUnionAgg<
            TYPE_STRING, AggregateFunctionHllSketchData<TYPE_STRING>>>(argument_types);

    AggregateDataPtr place =
            arena->aligned_alloc(agg_func->size_of_data(), agg_func->align_of_data());
    agg_func->create(place);

    auto buffer = ColumnString::create();
    BufferWritable w(*buffer);
    EXPECT_NO_THROW(agg_func->serialize(place, w)); // covers write() empty-state branch
    w.commit();

    AggregateDataPtr new_place =
            arena->aligned_alloc(agg_func->size_of_data(), agg_func->align_of_data());
    agg_func->create(new_place);

    BufferReadable r(buffer->get_data_at(0));
    agg_func->deserialize(new_place, r, *arena);

    ColumnInt64 result;
    agg_func->insert_result_into(new_place, result);
    EXPECT_DOUBLE_EQ(result.get_data()[0], 0.0);

    agg_func->destroy(place);
    agg_func->destroy(new_place);
}

TEST_F(AggregateFunctionDataSketchesHllUnionAggTest, testCorruptedInputThrows) {
    DataTypes argument_types = {std::make_shared<DataTypeString>()};
    auto agg_func = std::make_shared<AggregateFunctionDataSketchesHllUnionAgg<
            TYPE_STRING, AggregateFunctionHllSketchData<TYPE_STRING>>>(argument_types);

    auto column_string = ColumnString::create();
    column_string->insert_data("x", 1); // definitely not a valid sketch
    const IColumn* columns[1] = {column_string.get()};

    AggregateDataPtr place =
            arena->aligned_alloc(agg_func->size_of_data(), agg_func->align_of_data());
    agg_func->create(place);

    try {
        agg_func->add(place, columns, 0, *arena); // covers add() CORRUPTION catch branch
        FAIL() << "Expected doris::Exception";
    } catch (const doris::Exception& e) {
        EXPECT_EQ(e.code(), doris::ErrorCode::CORRUPTION);
    }

    auto corrupt_buf = ColumnString::create();
    BufferWritable corrupt_w(*corrupt_buf);
    StringRef corrupted("x", 1);
    corrupt_w.write_binary(corrupted);
    corrupt_w.commit();

    BufferReadable r(corrupt_buf->get_data_at(0));
    try {
        agg_func->deserialize(place, r, *arena); // covers read() CORRUPTION catch branch
        FAIL() << "Expected doris::Exception";
    } catch (const doris::Exception& e) {
        EXPECT_EQ(e.code(), doris::ErrorCode::CORRUPTION);
    }

    agg_func->destroy(place);
}

TEST_F(AggregateFunctionDataSketchesHllUnionAggTest, testAllocatorAwareSketchInput) {
    DataTypes argument_types = {std::make_shared<DataTypeString>()};
    using AggFunc =
            AggregateFunctionDataSketchesHllUnionAgg<TYPE_STRING,
                                                     AggregateFunctionHllSketchData<TYPE_STRING>>;
    auto agg_func = std::make_shared<AggFunc>(argument_types);

    using Alloc = doris::CustomStdAllocator<uint8_t>;
    using Sketch = datasketches::hll_sketch_alloc<Alloc>;

    Sketch sketch(8, datasketches::HLL_8, false, Alloc());
    for (int i = 0; i < 7; ++i) {
        sketch.update(i);
    }
    const auto ser = sketch.serialize_compact();

    auto column_string = ColumnString::create();
    column_string->insert_data((const char*)ser.data(), ser.size());
    const IColumn* columns[1] = {column_string.get()};

    AggregateDataPtr place =
            arena->aligned_alloc(agg_func->size_of_data(), agg_func->align_of_data());
    agg_func->create(place);

    agg_func->add(place, columns, 0, *arena);

    ColumnInt64 result;
    agg_func->insert_result_into(place, result);
    EXPECT_EQ(result.get_data()[0], static_cast<int64_t>(sketch.get_estimate()));

    agg_func->destroy(place);
}

} // namespace doris
