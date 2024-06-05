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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <memory>

#include "gtest/gtest_pred_impl.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/string_buffer.hpp"
#include "vec/core/types.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
namespace vectorized {
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

void register_aggregate_function_sequence_match(AggregateFunctionSimpleFactory& factory);

class VSequenceMatchTest : public testing::Test {
public:
    AggregateFunctionPtr agg_function_sequence_match;
    AggregateFunctionPtr agg_function_sequence_count;

    VSequenceMatchTest() {}

    void SetUp() {
        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        DataTypes data_types = {
                std::make_shared<DataTypeString>(), std::make_shared<DataTypeDateTime>(),
                std::make_shared<DataTypeUInt8>(), std::make_shared<DataTypeUInt8>(),
                std::make_shared<DataTypeUInt8>()};
        agg_function_sequence_match = factory.get("sequence_match", data_types, false);
        EXPECT_NE(agg_function_sequence_match, nullptr);
        agg_function_sequence_count = factory.get("sequence_count", data_types, false);
        EXPECT_NE(agg_function_sequence_count, nullptr);
    }

    void TearDown() {}
};

TEST_F(VSequenceMatchTest, testMatchEmpty) {
    std::unique_ptr<char[]> memory(new char[agg_function_sequence_match->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function_sequence_match->create(place);

    ColumnString buf;
    VectorBufferWriter buf_writer(buf);
    agg_function_sequence_match->serialize(place, buf_writer);
    buf_writer.commit();
    VectorBufferReader buf_reader(buf.get_data_at(0));
    agg_function_sequence_match->deserialize(place, buf_reader, nullptr);

    std::unique_ptr<char[]> memory2(new char[agg_function_sequence_match->size_of_data()]);
    AggregateDataPtr place2 = memory2.get();
    agg_function_sequence_match->create(place2);

    agg_function_sequence_match->merge(place, place2, nullptr);
    ColumnVector<UInt8> column_result;
    agg_function_sequence_match->insert_result_into(place, column_result);
    EXPECT_EQ(column_result.get_data()[0], 0);

    ColumnVector<UInt8> column_result2;
    agg_function_sequence_match->insert_result_into(place2, column_result2);
    EXPECT_EQ(column_result2.get_data()[0], 0);

    agg_function_sequence_match->destroy(place);
    agg_function_sequence_match->destroy(place2);
}

TEST_F(VSequenceMatchTest, testCountEmpty) {
    std::unique_ptr<char[]> memory(new char[agg_function_sequence_count->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function_sequence_count->create(place);

    ColumnString buf;
    VectorBufferWriter buf_writer(buf);
    agg_function_sequence_count->serialize(place, buf_writer);
    buf_writer.commit();
    VectorBufferReader buf_reader(buf.get_data_at(0));
    agg_function_sequence_count->deserialize(place, buf_reader, nullptr);

    std::unique_ptr<char[]> memory2(new char[agg_function_sequence_count->size_of_data()]);
    AggregateDataPtr place2 = memory2.get();
    agg_function_sequence_count->create(place2);

    agg_function_sequence_count->merge(place, place2, nullptr);
    ColumnVector<Int64> column_result;
    agg_function_sequence_count->insert_result_into(place, column_result);
    EXPECT_EQ(column_result.get_data()[0], 0);

    ColumnVector<Int64> column_result2;
    agg_function_sequence_count->insert_result_into(place2, column_result2);
    EXPECT_EQ(column_result2.get_data()[0], 0);

    agg_function_sequence_count->destroy(place);
    agg_function_sequence_count->destroy(place2);
}

TEST_F(VSequenceMatchTest, testMatchSerialize) {
    const int NUM_CONDS = 4;
    auto column_pattern = ColumnString::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        column_pattern->insert("(?1)(?2)");
    }

    auto column_timestamp = ColumnVector<Int64>::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        VecDateTimeValue time_value;
        time_value.unchecked_set_time(2022, 11, 2, 0, 0, i);
        column_timestamp->insert_data((char*)&time_value, 0);
    }

    auto column_event1 = ColumnVector<UInt8>::create();
    column_event1->insert(1);
    column_event1->insert(0);
    column_event1->insert(0);
    column_event1->insert(0);

    auto column_event2 = ColumnVector<UInt8>::create();
    column_event2->insert(0);
    column_event2->insert(1);
    column_event2->insert(0);
    column_event2->insert(0);

    auto column_event3 = ColumnVector<UInt8>::create();
    column_event3->insert(0);
    column_event3->insert(0);
    column_event3->insert(1);
    column_event3->insert(0);

    std::unique_ptr<char[]> memory(new char[agg_function_sequence_match->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function_sequence_match->create(place);
    const IColumn* column[5] = {column_pattern.get(), column_timestamp.get(), column_event1.get(),
                                column_event2.get(), column_event3.get()};
    for (int i = 0; i < NUM_CONDS; i++) {
        agg_function_sequence_match->add(place, column, i, nullptr);
    }

    ColumnString buf;
    VectorBufferWriter buf_writer(buf);
    agg_function_sequence_match->serialize(place, buf_writer);
    buf_writer.commit();

    std::unique_ptr<char[]> memory2(new char[agg_function_sequence_match->size_of_data()]);
    AggregateDataPtr place2 = memory2.get();
    agg_function_sequence_match->create(place2);

    VectorBufferReader buf_reader(buf.get_data_at(0));
    agg_function_sequence_match->deserialize(place2, buf_reader, nullptr);

    ColumnVector<UInt8> column_result;
    agg_function_sequence_match->insert_result_into(place, column_result);
    EXPECT_EQ(column_result.get_data()[0], 1);
    agg_function_sequence_match->destroy(place);

    ColumnVector<UInt8> column_result2;
    agg_function_sequence_match->insert_result_into(place2, column_result2);
    EXPECT_EQ(column_result2.get_data()[0], 1);
    agg_function_sequence_match->destroy(place2);
}

TEST_F(VSequenceMatchTest, testCountSerialize) {
    AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
    DataTypes data_types = {std::make_shared<DataTypeString>(),
                            std::make_shared<DataTypeDateTime>(), std::make_shared<DataTypeUInt8>(),
                            std::make_shared<DataTypeUInt8>()};
    agg_function_sequence_count = factory.get("sequence_count", data_types, false);
    EXPECT_NE(agg_function_sequence_count, nullptr);

    const int NUM_CONDS = 4;
    auto column_pattern = ColumnString::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        column_pattern->insert("(?1)(?2)");
    }

    auto column_timestamp = ColumnVector<Int64>::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        VecDateTimeValue time_value;
        time_value.unchecked_set_time(2022, 11, 2, 0, 0, i);
        column_timestamp->insert_data((char*)&time_value, 0);
    }

    auto column_event1 = ColumnVector<UInt8>::create();
    column_event1->insert(1);
    column_event1->insert(0);
    column_event1->insert(1);
    column_event1->insert(0);

    auto column_event2 = ColumnVector<UInt8>::create();
    column_event2->insert(0);
    column_event2->insert(1);
    column_event2->insert(0);
    column_event2->insert(1);

    std::unique_ptr<char[]> memory(new char[agg_function_sequence_count->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function_sequence_count->create(place);
    const IColumn* column[4] = {column_pattern.get(), column_timestamp.get(), column_event1.get(),
                                column_event2.get()};
    for (int i = 0; i < NUM_CONDS; i++) {
        agg_function_sequence_count->add(place, column, i, nullptr);
    }

    ColumnString buf;
    VectorBufferWriter buf_writer(buf);
    agg_function_sequence_count->serialize(place, buf_writer);
    buf_writer.commit();

    std::unique_ptr<char[]> memory2(new char[agg_function_sequence_count->size_of_data()]);
    AggregateDataPtr place2 = memory2.get();
    agg_function_sequence_count->create(place2);

    VectorBufferReader buf_reader(buf.get_data_at(0));
    agg_function_sequence_count->deserialize(place2, buf_reader, nullptr);

    ColumnVector<Int64> column_result;
    agg_function_sequence_count->insert_result_into(place, column_result);
    EXPECT_EQ(column_result.get_data()[0], 2);
    agg_function_sequence_count->destroy(place);

    ColumnVector<Int64> column_result2;
    agg_function_sequence_count->insert_result_into(place2, column_result2);
    EXPECT_EQ(column_result2.get_data()[0], 2);
    agg_function_sequence_count->destroy(place2);
}

TEST_F(VSequenceMatchTest, testMatchReverseSortedSerializeMerge) {
    AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
    DataTypes data_types = {std::make_shared<DataTypeString>(),
                            std::make_shared<DataTypeDateTime>(), std::make_shared<DataTypeUInt8>(),
                            std::make_shared<DataTypeUInt8>()};
    agg_function_sequence_match = factory.get("sequence_match", data_types, false);
    EXPECT_NE(agg_function_sequence_match, nullptr);

    const int NUM_CONDS = 2;
    auto column_pattern = ColumnString::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        column_pattern->insert("(?1)(?2)");
    }

    auto column_timestamp = ColumnVector<Int64>::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        VecDateTimeValue time_value;
        time_value.unchecked_set_time(2022, 11, 2, 0, 0, NUM_CONDS - i);
        column_timestamp->insert_data((char*)&time_value, 0);
    }

    auto column_event1 = ColumnVector<UInt8>::create();
    column_event1->insert(0);
    column_event1->insert(1);

    auto column_event2 = ColumnVector<UInt8>::create();
    column_event2->insert(0);
    column_event2->insert(0);

    std::unique_ptr<char[]> memory(new char[agg_function_sequence_match->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function_sequence_match->create(place);
    const IColumn* column[4] = {column_pattern.get(), column_timestamp.get(), column_event1.get(),
                                column_event2.get()};
    for (int i = 0; i < NUM_CONDS; i++) {
        agg_function_sequence_match->add(place, column, i, nullptr);
    }

    ColumnString buf;
    VectorBufferWriter buf_writer(buf);
    agg_function_sequence_match->serialize(place, buf_writer);
    buf_writer.commit();
    agg_function_sequence_match->destroy(place);

    std::unique_ptr<char[]> memory2(new char[agg_function_sequence_match->size_of_data()]);
    AggregateDataPtr place2 = memory2.get();
    agg_function_sequence_match->create(place2);

    VectorBufferReader buf_reader(buf.get_data_at(0));
    agg_function_sequence_match->deserialize(place2, buf_reader, nullptr);

    ColumnVector<UInt8> column_result;
    agg_function_sequence_match->insert_result_into(place2, column_result);
    EXPECT_EQ(column_result.get_data()[0], 0);

    auto column_timestamp2 = ColumnVector<Int64>::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        VecDateTimeValue time_value;
        time_value.unchecked_set_time(2022, 11, 2, 0, 1, NUM_CONDS - i);
        column_timestamp2->insert_data((char*)&time_value, 0);
    }

    auto column_event3 = ColumnVector<UInt8>::create();
    column_event3->insert(0);
    column_event3->insert(0);

    auto column_event4 = ColumnVector<UInt8>::create();
    column_event4->insert(0);
    column_event4->insert(1);

    std::unique_ptr<char[]> memory3(new char[agg_function_sequence_match->size_of_data()]);
    AggregateDataPtr place3 = memory3.get();
    agg_function_sequence_match->create(place3);
    const IColumn* column2[4] = {column_pattern.get(), column_timestamp2.get(), column_event3.get(),
                                 column_event4.get()};
    for (int i = 0; i < NUM_CONDS; i++) {
        agg_function_sequence_match->add(place3, column2, i, nullptr);
    }

    agg_function_sequence_match->merge(place2, place3, nullptr);

    ColumnVector<UInt8> column_result2;
    agg_function_sequence_match->insert_result_into(place2, column_result2);
    EXPECT_EQ(column_result2.get_data()[0], 1);

    agg_function_sequence_match->destroy(place2);
    agg_function_sequence_match->destroy(place3);
}

TEST_F(VSequenceMatchTest, testCountReverseSortedSerializeMerge) {
    AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
    DataTypes data_types = {std::make_shared<DataTypeString>(),
                            std::make_shared<DataTypeDateTime>(), std::make_shared<DataTypeUInt8>(),
                            std::make_shared<DataTypeUInt8>()};
    agg_function_sequence_count = factory.get("sequence_count", data_types, false);
    EXPECT_NE(agg_function_sequence_count, nullptr);

    const int NUM_CONDS = 2;
    auto column_pattern = ColumnString::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        column_pattern->insert("(?1)(?2)");
    }

    auto column_timestamp = ColumnVector<Int64>::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        VecDateTimeValue time_value;
        time_value.unchecked_set_time(2022, 11, 2, 0, 0, NUM_CONDS - i);
        column_timestamp->insert_data((char*)&time_value, 0);
    }

    auto column_event1 = ColumnVector<UInt8>::create();
    column_event1->insert(0);
    column_event1->insert(1);

    auto column_event2 = ColumnVector<UInt8>::create();
    column_event2->insert(1);
    column_event2->insert(0);

    std::unique_ptr<char[]> memory(new char[agg_function_sequence_count->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function_sequence_count->create(place);
    const IColumn* column[4] = {column_pattern.get(), column_timestamp.get(), column_event1.get(),
                                column_event2.get()};
    for (int i = 0; i < NUM_CONDS; i++) {
        agg_function_sequence_count->add(place, column, i, nullptr);
    }

    ColumnString buf;
    VectorBufferWriter buf_writer(buf);
    agg_function_sequence_count->serialize(place, buf_writer);
    buf_writer.commit();
    agg_function_sequence_count->destroy(place);

    std::unique_ptr<char[]> memory2(new char[agg_function_sequence_count->size_of_data()]);
    AggregateDataPtr place2 = memory2.get();
    agg_function_sequence_count->create(place2);

    VectorBufferReader buf_reader(buf.get_data_at(0));
    agg_function_sequence_count->deserialize(place2, buf_reader, nullptr);

    ColumnVector<Int64> column_result;
    agg_function_sequence_count->insert_result_into(place2, column_result);
    EXPECT_EQ(column_result.get_data()[0], 1);

    auto column_timestamp2 = ColumnVector<Int64>::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        VecDateTimeValue time_value;
        time_value.unchecked_set_time(2022, 11, 2, 0, 1, NUM_CONDS - i);
        column_timestamp2->insert_data((char*)&time_value, 0);
    }

    auto column_event3 = ColumnVector<UInt8>::create();
    column_event3->insert(0);
    column_event3->insert(1);

    auto column_event4 = ColumnVector<UInt8>::create();
    column_event4->insert(1);
    column_event4->insert(0);

    std::unique_ptr<char[]> memory3(new char[agg_function_sequence_count->size_of_data()]);
    AggregateDataPtr place3 = memory3.get();
    agg_function_sequence_count->create(place3);
    const IColumn* column2[4] = {column_pattern.get(), column_timestamp2.get(), column_event3.get(),
                                 column_event4.get()};
    for (int i = 0; i < NUM_CONDS; i++) {
        agg_function_sequence_count->add(place3, column2, i, nullptr);
    }

    agg_function_sequence_count->merge(place2, place3, nullptr);

    ColumnVector<Int64> column_result2;
    agg_function_sequence_count->insert_result_into(place2, column_result2);
    EXPECT_EQ(column_result2.get_data()[0], 2);

    agg_function_sequence_count->destroy(place2);
    agg_function_sequence_count->destroy(place3);
}

} // namespace doris::vectorized